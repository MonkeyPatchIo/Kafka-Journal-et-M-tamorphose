package io.monkeypatch.kafka.metamorphosis;

import io.monkeypatch.kafka.metamorphosis.utils.TopologyBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.monkeypatch.kafka.workshop.serde.JsonSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Collectors;

class Chapter02_BranchingAndMerging extends TopologyBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter02_BranchingAndMerging.class);

    private static final UUID APPLICATION_ID = UUID.randomUUID();
    private static final String INPUT_TOPIC  = String.format("sentences-input-%s", APPLICATION_ID);
    private static final String OUTPUT_TOPIC = String.format("sentences-output-%s", APPLICATION_ID);
    private static final String ERROR_TOPIC = String.format("sentences-error-%s", APPLICATION_ID);

    @Override
    protected void buildTopology(StreamsBuilder builder) {
        KStream<Integer, Sentence>[] branches = builder
            .stream(
                INPUT_TOPIC,
                Consumed.with(new JsonSerde.IntegerSerde(), new Sentence.Serde())
            )
            .branch(
                (chapter, sentence) -> sentence.getText().length() < 80,
                (chapter, sentence) -> sentence.getText().length() > 120,
                (chapter, sentence) -> true
            );

        KStream<Integer, Sentence> shortSentences = branches[0];
        KStream<Integer, Sentence> longSentences = branches[1];

        shortSentences.merge(longSentences)
            .mapValues(Sentence::getText)
            .to(
                ERROR_TOPIC,
                Produced.with(new JsonSerde.IntegerSerde(), new JsonSerde.StringSerde())
            );

        KStream<Integer, Sentence> averageSentences = branches[2];

        averageSentences
            .mapValues(Sentence::getText)
            .to(
                OUTPUT_TOPIC,
                Produced.with(new JsonSerde.IntegerSerde(), new JsonSerde.StringSerde())
            );
    }

    @Test
    public void test() throws Exception {
        replayKafkaTrial(INPUT_TOPIC);

        buildTopologyAndStart(APPLICATION_ID);

        assertValuesReceived(
            OUTPUT_TOPIC,
            Files.lines(Path.of("src/test/resources/chapter02.output"))
                .collect(Collectors.toList())
        );

        assertValuesReceived(
            ERROR_TOPIC,
            Files.lines(Path.of("src/test/resources/chapter02.errors"))
                .collect(Collectors.toList())
        );
    }
}
