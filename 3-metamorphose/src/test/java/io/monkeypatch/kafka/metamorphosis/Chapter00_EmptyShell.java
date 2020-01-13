package io.monkeypatch.kafka.metamorphosis;

import io.monkeypatch.kafka.workshop.journal.workshop.model.Sentence;
import io.monkeypatch.kafka.workshop.journal.workshop.serde.JsonSerde;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import io.vavr.collection.Vector;
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
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * This class so you can see all the boilerplate once and for all in a dummy use case.
 *
 * Other tests use the "boilerplate" class so we don't have to think about it.
 */
class Chapter00_EmptyShell {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter00_EmptyShell.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9093";

    private static final String APPLICATION_ID = UUID.randomUUID().toString();
    private static final String INPUT_TOPIC  = String.format("sentences-input-%s", APPLICATION_ID);
    private static final String OUTPUT_TOPIC = String.format("sentences-output-%s", APPLICATION_ID);

    private static KafkaStreams kafkaStreams;

    @BeforeAll
    static void setUp() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .stream(
                INPUT_TOPIC,
                Consumed.with(new JsonSerde.StringSerde(), new Sentence.Serde())
            )
            .peek((k, v) -> LOG.info("Replay: {}", v.getText()))
            .to(OUTPUT_TOPIC);

        final Topology topology = streamsBuilder.build();
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
    }

    @Test
    void test() throws InterruptedException {

        //<editor-fold desc="Instantiate Input Producer">
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerde.StringSerde.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class.getName());

        KafkaProducer<String, Sentence> producer = new KafkaProducer<>(properties);
        //</editor-fold>

        var chapters = Vector.of(
            Tuple.of("Chapter 1",  "Arrest - Conversation with Mrs. Grubach - Then Miss Bürstner"),
            Tuple.of("Chapter 2",  "First Cross-examination"),
            Tuple.of("Chapter 3",  "In the empty Courtroom - The Student - The Offices"),
            Tuple.of("Chapter 4",  "Miss Bürstner's Friend"),
            Tuple.of("Chapter 5",  "The whip-man"),
            Tuple.of("Chapter 6",  "K.'s uncle - Leni"),
            Tuple.of("Chapter 7",  "Lawyer - Manufacturer - Painter"),
            Tuple.of("Chapter 8",  "Block, the businessman  - Dismissing the lawyer"),
            Tuple.of("Chapter 9",  "In the Cathedral"),
            Tuple.of("Chapter 10", "End")
        );

        //<editor-fold desc="Send Input Messages">
        chapters.toStream()
            .zipWith(
                Stream.iterate(1, i -> i+1),
                (t, i) -> t.map2(s -> new Sentence(i, "Trial", i, s))
            )
            .forEach(kv -> {
                String key = kv._1;
                Sentence value = kv._2;
                LOG.info("######## {} - {}={}", INPUT_TOPIC, key, value);
                producer.send(new ProducerRecord<>(INPUT_TOPIC, key, value));
            });
        producer.close();
        //</editor-fold>

        kafkaStreams.start();

        //<editor-fold desc="Instantiate Output Consumer">
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonSerde.StringSerde.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Sentence.Serde.class.getName());

        Consumer<String, Sentence> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
        //</editor-fold>

        List<Tuple2<String, String>> results = new ArrayList<>(chapters.length());

        //<editor-fold desc="Consume Stream Output and collect results">
        CountDownLatch consumedLatch = new CountDownLatch(chapters.length());
        var stop = new AtomicBoolean();
        new Thread(() -> {
            while(!stop.get()) {
                ConsumerRecords<String, Sentence> record = consumer.poll(Duration.ofSeconds(10));
                record.iterator()
                    .forEachRemaining(r -> {
                        consumedLatch.countDown();
                        results.add(Tuple.of(r.key(), r.value().getText()));
                    });
            }
            consumer.close();
        }).start();

        consumedLatch.await();
        stop.set(false);
        //</editor-fold>

        assertIterableEquals(results, chapters);
    }
}
