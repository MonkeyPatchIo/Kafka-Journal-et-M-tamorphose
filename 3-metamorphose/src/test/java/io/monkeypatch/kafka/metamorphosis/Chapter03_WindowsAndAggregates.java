package io.monkeypatch.kafka.metamorphosis;

import io.monkeypatch.kafka.metamorphosis.utils.TopologyBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.monkeypatch.kafka.workshop.serde.JsonSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

class Chapter03_WindowsAndAggregates extends TopologyBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter03_WindowsAndAggregates.class);

    private static final UUID APPLICATION_ID = UUID.randomUUID();
    private static final String INPUT_TOPIC  = String.format("sentences-input-%s", APPLICATION_ID);
    private static final String OUTPUT_TOPIC = String.format("sentences-output-%s", APPLICATION_ID);

    private static final int TIME_WINDOW_MILLIS = 5;

    @Override
    protected void buildTopology(StreamsBuilder builder) {
        builder
            .stream(
                INPUT_TOPIC,
                Consumed.with(new JsonSerde.IntegerSerde(), new Sentence.Serde())
            )
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMillis(TIME_WINDOW_MILLIS)))
            .count()
            .toStream((wk,count) -> wk.key())
            .peek((c, cc) -> LOG.info("CHAPTER {} - VALUE={}", c, cc))
            .to(
                OUTPUT_TOPIC,
                Produced.with(new JsonSerde.IntegerSerde(), new JsonSerde.LongSerde())
            );
    }

    @Test
    public void test() throws Exception {
        Random random = new Random();
        replayKafkaTrial(INPUT_TOPIC, random);

        buildTopologyAndStart(APPLICATION_ID);

        assertTotal(OUTPUT_TOPIC, 1233);
    }
}
