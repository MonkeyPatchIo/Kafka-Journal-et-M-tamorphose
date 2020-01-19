package io.monkeypatch.kafka.workshop.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.kafka.workshop.serde.BaseJsonSerde;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Sentence {

    public static class Serde extends BaseJsonSerde<Sentence> {
        public Serde() { super(Sentence.class); }
    }
    public static final Serde SERDE = new Serde();

    @JsonProperty("id") private final int id;
    @JsonProperty("b") private final String book;
    @JsonProperty("c") private final int chapter;
    @JsonProperty("t") private final String text;

    @JsonCreator
    public Sentence(
            @JsonProperty("id") int id,
            @JsonProperty("b") String book,
            @JsonProperty("c") int chapter,
            @JsonProperty("t") String text) {
        this.id = id;
        this.book = book;
        this.chapter = chapter;
        this.text = text;
    }

    public int getId() {
        return id;
    }

    public String getBook() {
        return book;
    }

    public int getChapter() {
        return chapter;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return SERDE.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sentence sentence = (Sentence) o;
        return new EqualsBuilder()
                .append(getId(), sentence.getId())
                .append(getChapter(), sentence.getChapter())
                .append(getBook(), sentence.getBook())
                .append(getText(), sentence.getText())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getId())
                .append(getBook())
                .append(getChapter())
                .append(getText())
                .toHashCode();
    }

    public static Stream<Sentence> fromBook(FranzKafkaBook book) {
        Serde serde = new Serde();
        try {
            InputStream is = ClassLoader.getSystemResourceAsStream("messages/" + book.name());
            Iterator<Object> lineIterator = IOUtils.lineIterator(is, "UTF-8");
            var spliterator = Spliterators.spliteratorUnknownSize(lineIterator, Spliterator.ORDERED);
            return StreamSupport.stream(spliterator, false)
                    .map(String.class::cast)
                    .map(s -> serde.deserialize(null, s.getBytes()))
                    .onClose(() -> IOUtils.closeQuietly(is));
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Sentence> fromAllBooks() {
        return Stream.of(FranzKafkaBook.values()).flatMap(Sentence::fromBook);
    }

    public static Stream<Sentence> firstNSentencesByChapter(int n) {
        return io.vavr.collection.Stream.ofAll(fromAllBooks())
            .groupBy(s -> s.getBook() + s.getChapter())
            .values()
            .map(ss -> ss.take(n))
            .reduce((a,b) -> io.vavr.collection.Stream.concat(a,b))
            .asJava().stream();
    }


    public static void main(String[] args) {
        Sentence.fromBook(FranzKafkaBook.metamorphosis)
                .forEach(System.out::println);
    }

}
