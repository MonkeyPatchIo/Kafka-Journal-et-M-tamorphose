package io.monkeypatch.kafka.metamorphosis;

import io.monkeypatch.kafka.metamorphosis.utils.TopologyBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.monkeypatch.kafka.workshop.serde.JsonSerde;
import io.vavr.collection.Stream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

class Chapter01_SimpleOperators extends TopologyBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter01_SimpleOperators.class);

    private static final UUID APPLICATION_ID = UUID.randomUUID();
    private static final String INPUT_TOPIC  = String.format("sentences-input-%s", APPLICATION_ID);
    private static final String OUTPUT_TOPIC = String.format("sentences-output-%s", APPLICATION_ID);

    @Override
    protected void buildTopology(StreamsBuilder builder) {
        builder
            .stream(
                INPUT_TOPIC,
                Consumed.with(new JsonSerde.IntegerSerde(), new Sentence.Serde())
            )
            .flatMapValues(s -> Arrays.asList(s.getText().split("\\s")))
            .mapValues(w -> Stream
                .ofAll(w.chars().mapToObj(c -> (char) c))
                .zipWith(Stream.iterate(0, i -> i+1)
                        .map(i -> i%2==0
                            ? (Function<Character, Character>) Character::toUpperCase
                            : (Function<Character, Character>) Character::toLowerCase),
                    (c, f) -> f.apply(c)
                )
                .collect(Collector.of(
                    StringBuilder::new,
                    StringBuilder::append,
                    StringBuilder::append,
                    StringBuilder::toString)
                )
            )
            .filter((k, s) -> s.length() > 2)
            .to(
                OUTPUT_TOPIC,
                Produced.with(new JsonSerde.IntegerSerde(), new JsonSerde.StringSerde())
            );
    }

    @Test
    public void test() throws Exception {
        replayKafkaMetamorphosis(INPUT_TOPIC);

        buildTopologyAndStart(APPLICATION_ID);

        assertValuesReceived(
            OUTPUT_TOPIC,
            Files.lines(Path.of("src/test/resources/chapter01.result"))
                .collect(Collectors.toList())
        );
    }
}
