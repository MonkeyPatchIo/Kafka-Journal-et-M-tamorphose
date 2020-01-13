package io.monkeypatch.kafka.workshop.parsebooks;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.monkeypatch.kafka.workshop.model.FranzKafkaBook;
import io.vavr.Tuple;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseKafkaBooks {

    private static final Logger LOG = LoggerFactory.getLogger(ParseKafkaBooks.class);
    public static final Pattern CHAPTER_LINE_PATTERN = Pattern.compile("^Chapter (?<chapter>\\d+).*$");

    static abstract class Context {
        private final int chapter;
        private final String rest;
        public Context(int chapter, String rest) {
            this.chapter = chapter;
            this.rest = rest;
        }
        public int getChapter() { return chapter; }
        public String getRest() { return rest; }
    }
    static abstract class Sentence extends Context {
        public Sentence(int chapter, String rest) {
            super(chapter, rest);
        }
        abstract boolean finished();
    }
    static class NotFinished extends Sentence {
        public NotFinished(int chapter, String rest) { super(chapter, rest); }
        @Override boolean finished() { return false; }
    }
    static class Finished extends Sentence {
        private final List<String> content;
        public Finished(List<String> content, int chapter, String rest) {
            super(chapter, rest);
            this.content = content;
        }
        public List<String> getContent() { return content; }
        @Override boolean finished() { return true; }
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AtomicInteger idCounter = new AtomicInteger();
        Stream.of(FranzKafkaBook.values())
            .map(Enum::name)
            .forEach(book -> Try.run(() -> {
                Path bookPath = Path.of("src/main/resources/books/" + book);
                System.out.println("Reading book from file " + bookPath.toFile().getAbsolutePath());

                String jsonLines = Stream.ofAll(Files.lines(bookPath))
                        .scanLeft(
                                new NotFinished(0, ""),
                                ParseKafkaBooks::parseLine
                        )
                        .filter(Finished.class::isInstance)
                        .map(Finished.class::cast)
                        .flatMap(f -> f.content.map(s -> Tuple.of(f.getChapter(), s)))
                        .map(chse -> new io.monkeypatch.kafka.workshop.model.Sentence(idCounter.incrementAndGet(), book, chse._1, chse._2))
                        .map(s -> Try.of(() -> mapper.writeValueAsString(s)).get() + "\n")
                        .foldLeft("", (acc, json) -> acc + json);
                File msgFile = new File("src/main/resources/messages/" + book);
                System.out.println("Writing msgs to file " + msgFile.getAbsolutePath());
                try (FileWriter output = new FileWriter(msgFile)) {
                    IOUtils.write(jsonLines, output);
                }
            }).onFailure(t -> t.printStackTrace()));
    }

    private static Sentence parseLine(Sentence acc, String line) {
        Matcher matcher = CHAPTER_LINE_PATTERN.matcher(line);
        if (line.isBlank()) {
            return new NotFinished(acc.getChapter(), "");
        }
        else if (matcher.matches()) {
            return new NotFinished(Integer.parseInt(matcher.group("chapter")), acc.getRest());
        }
        else {
            List<String> result = List.empty();

            String rest = (acc.getRest().trim() + " " + line.trim())
                    .replaceAll("[\"]", "")
                    .replaceAll("(Mrs?|K)\\.", "$1");
            boolean matched = true;
            Pattern sentencePattern = Pattern.compile("^(?<s>[^.!?]*?(?:\\.|\\?|!)+(?!,| [a-z]))(?<r>.*)$");
            while(matched) {
                Matcher sMatcher = sentencePattern.matcher(rest);
                if (sMatcher.matches()) {
                    result = result.append(sMatcher.group("s").trim());
                    rest = sMatcher.group("r").trim();
                }
                else {
                    matched = false;
                }
            }
            if (result.size() > 0) {
                return new Finished(result, acc.getChapter(), rest);
            }
            else {
                return new NotFinished(acc.getChapter(), rest);
            }
        }

    }

}
