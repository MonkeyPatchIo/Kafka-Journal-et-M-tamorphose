package io.monkeypatch.kafka.metamorphosis;

import io.monkeypatch.kafka.metamorphosis.utils.TopologyBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.monkeypatch.kafka.workshop.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

class Chapter04_StatefulOperators extends TopologyBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter04_StatefulOperators.class);

    private static final UUID APPLICATION_ID = UUID.randomUUID();
    private static final String INPUT_TOPIC  = String.format("sentences-input-%s", APPLICATION_ID);
    private static final String OUTPUT_TOPIC = String.format("sentences-output-%s", APPLICATION_ID);

    private static final String CHAPTER_STORE_NAME = "chapter-store";
    private static final String CHAPTER_STORE_PREFIX = "CHAPTER-%s";

    @Override
    protected void buildTopology(StreamsBuilder builder) {
        KeyValueBytesStoreSupplier storeSupplier =
            Stores.inMemoryKeyValueStore(CHAPTER_STORE_NAME);

        StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
            Stores.keyValueStoreBuilder(
                storeSupplier,
                Serdes.String(),
                Serdes.Long());

        builder.addStateStore(storeBuilder);
        builder
            .stream(
                INPUT_TOPIC,
                Consumed.with(new JsonSerde.IntegerSerde(), new Sentence.Serde())
            )
            .transformValues(() -> new ChapterTransformer(), CHAPTER_STORE_NAME)
            .mapValues((k, v) -> String.format("K=%s ; V=%s", k, v))
            .to(
                OUTPUT_TOPIC,
                Produced.with(new JsonSerde.IntegerSerde(), new JsonSerde.StringSerde())
            );

    }

    static class ChapterTransformer implements ValueTransformer<Sentence, Long> {

        private KeyValueStore<String, Long> stateStore;

        @Override
        public void init(ProcessorContext context) {
            stateStore = (KeyValueStore<String, Long>) context.getStateStore(CHAPTER_STORE_NAME);
        }

        @Override
        public Long transform(Sentence value) {
            var key = String.format(CHAPTER_STORE_PREFIX, value.getChapter());
            return Optional.ofNullable(stateStore.get(key))
                .map(prev -> {
                    long current = prev + 1;
                    stateStore.put(key, current);
                    return current;
                })
                .orElseGet(() -> {
                    long current = 1;
                    stateStore.put(key, current);
                    return current;
                });
        }

        @Override
        public void close() {}
    }

    @Test
    public void test() throws Exception {
        Random random = new Random();
        replayKafkaTrial(INPUT_TOPIC, random);

        buildTopologyAndStart(APPLICATION_ID);

        assertValuesReceived(
            OUTPUT_TOPIC,
            Files.lines(Path.of("src/test/resources/chapter04.result"))
                .collect(Collectors.toList())
        );
    }
}
