package io.monkeypatch.kafka.metamorphosis.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.monkeypatch.kafka.workshop.serde.JsonSerde;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public abstract class TopologyBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyBoilerplate.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9093";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    protected abstract void buildTopology(StreamsBuilder builder);

    protected void buildTopologyAndStart(UUID applicationId) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId.toString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        buildTopology(streamsBuilder);

        final Topology topology = streamsBuilder.build();
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.start();
    }

    //<editor-fold desc="Send book JSON lines into INPUT_TOPIC">
    private KafkaProducer<Integer, Sentence> makeProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerde.IntegerSerde.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class.getName());

        return new KafkaProducer<>(properties);
    }

    private Path bookPath(String book) {
        return Path.of("../2-journal/journal-model/src/main/resources/messages/" + book);
    }

    private void replayBook(String book, String topic) throws IOException {
        var producer = makeProducer();
        var path = bookPath(book);
        Stream.ofAll(Files.lines(path))
            .map(s -> Try.of(() -> MAPPER.readValue(s, Sentence.class)).get())
            .map(s -> new ProducerRecord<>(topic, s.getChapter(), s))
            .peek(r -> LOG.info("######## {} - {}={}", topic, r.key(), r.value()))
            .forEach(producer::send);
    }

    protected void replayKafkaMetamorphosis(String topic)
        throws IOException
    {
        replayBook("metamorphosis", topic);
    }

    protected void replayKafkaTrial(String topic) throws IOException {
        replayBook("trial", topic);
    }
    //</editor-fold>

    private Consumer<Integer, String> makeConsumer() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonSerde.IntegerSerde.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonSerde.StringSerde.class.getName());

        return new KafkaConsumer<>(consumerConfig);
    }

    private <T> List<Tuple2<Integer, String>> assertReceived(String topic, int expectedCount) {
        List<Tuple2<Integer, String>> results = new ArrayList<>();

        var consumedLatch = new CountDownLatch(expectedCount);
        var stop = new AtomicBoolean();
        var consumer = makeConsumer();
        consumer.subscribe(Collections.singleton(topic));
        new Thread(() -> {
            while(!stop.get()) {
                ConsumerRecords<Integer, String> record = consumer.poll(Duration.ofSeconds(10));
                record.iterator()
                    .forEachRemaining(r -> {
                        results.add(Tuple.of(r.key(), r.value()));
                        consumedLatch.countDown();
                    });
            }
            consumer.close();
        }).start();

        assertDoesNotThrow(
            () -> assertTrue(consumedLatch.await(5L, TimeUnit.SECONDS)),
            () -> {
                stop.set(false);
                return String.format("Expected %s but received %s after 5 second.\n[%s\n]",
                    expectedCount,
                    expectedCount - consumedLatch.getCount(),
                    results.stream().map(Tuple2::toString).collect(Collectors.joining(",\n")));
            }
        );
        stop.set(false);

        return results;
    }

    protected void assertValuesReceived(String topic, List<String> expected) {
        List<Tuple2<Integer, String>> results = assertReceived(topic, expected.size());

        assertIterableEquals(expected, results.stream().map(Tuple2::_2).collect(Collectors.toList()));
    }
}
