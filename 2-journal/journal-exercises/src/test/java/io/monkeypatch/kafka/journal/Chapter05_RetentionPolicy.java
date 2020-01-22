package io.monkeypatch.kafka.journal;

import io.monkeypatch.kafka.journal.utils.KakfaBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.collection.HashMap;
import io.vavr.control.Try;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class Chapter05_RetentionPolicy extends KakfaBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter05_RetentionPolicy.class);

    String sourceTopic = topicName();
    Supplier<Stream<Sentence>> sentences = () -> Sentence.fromAllBooks();
    Integer sentenceCount = io.vavr.collection.Stream.ofAll(sentences.get()).map(s -> 1).sum().intValue();

    @BeforeEach
    void createTopic() {
        try {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            AdminClient admin = KafkaAdminClient.create(config);
            NewTopic topic = new NewTopic(sourceTopic, 3, (short)1);
            topic.configs(HashMap.of(
                    TopicConfig.RETENTION_BYTES_CONFIG, "1024", // 1kb allowed per partition
                    TopicConfig.RETENTION_MS_CONFIG, "5000" // 5 seconds...
            ).toJavaMap());
            admin.createTopics(List.of(topic)).all().get();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    void testRetentionPolicy() throws Exception {
        int rand = random.nextInt();
        String groupId = String.format("test-group-%d", rand);
        Properties config = consumerConfig(groupId);
        CountDownLatch latch = new CountDownLatch(1);

        // We will store the number of consumed sentences here
        AtomicInteger consumedSentences = new AtomicInteger();

        // Start producing to the topics asynchronously, choosing a specific topic for each sentence
        runProducer(s -> sourceTopic, sentences.get(), this::getKey, false);

        // Leave some time for compaction to happen.
        // We don't control exactly when the server will do compaction.
        // Leaving a smaller time makes the test fail.
        Thread.sleep( 10_000);
        //Thread.sleep(   100);

        // Run a single consumer, just to see what it gets
        runConsumer(sourceTopic, config, latch, consumedSentences);

        // Wait for the consumer to finish...
        assertThat(latch.await(2, TimeUnit.MINUTES)).isTrue();
        // We must have lost some messages!
        assertThat(consumedSentences.get()).isLessThan(sentenceCount);
    }

    private void runConsumer(String topicName, Properties config, CountDownLatch latch, AtomicInteger consumedSentences) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            int emptyPolls = 0;
            Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);
            try {
                consumer.subscribe(List.of(topicName));
                do {
                    ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofMillis(500));
                    if (records.count() == 0) {
                        emptyPolls += 1;
                    }
                    else {
                        emptyPolls = 0;
                        Thread.sleep(200);
                    }
                    for (ConsumerRecord<Integer, Sentence> record : records) {
                        consumedSentences.incrementAndGet();
                        LOG.info("CONSUMED {} key={} msg={}", msgId(record), record.key(), record.value());
                        registerRecordInPartitionFiles(record, "target/ch05/");
                    }

                } while (emptyPolls < 10);
                // We exit the loop when 10 successive polls have yielded no message.
            }
            catch(Exception e) {
                LOG.error(e.getMessage(), e);
            }
            finally {
                latch.countDown();
                consumer.close();
                executor.shutdown();
            }
        });
    }


    private Properties consumerConfig(String groupId) {
        Properties config = new Properties();
        // This property is of some significance here.
        // Wishing the earliest messages may have been erased.
        // Using latest should allow to consume everything from the time
        // we start consuming, unless our treatment for each message
        // is too long...
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // We make sure we don't consume too fast...
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "1");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, "1");
        return config;
    }

    public void runProducer(
            Function<Sentence, String> destTopicFn,
            Stream<Sentence> sentences,
            Function<Sentence, Integer> keyExtractor,
            boolean silent
    ) {
        Executors.newSingleThreadExecutor().submit(() -> {
            Producer<Integer, Sentence> producer = new KafkaProducer<>(producerConfig());
            sentences.forEach(s -> {
                Integer key = keyExtractor.apply(s);
                Try.of(() -> producer.send(new ProducerRecord<>(destTopicFn.apply(s), key, s)).get())
                    .peek(md -> { if(!silent) { LOG.info("PRODUCED {} key={} msg={}", msgId(md), key, s); }})
                    .onFailure(e -> LOG.error(e.getMessage(), e));
                Try.run(() -> Thread.sleep(30));
            });
        });
    }

    private Properties producerConfig() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, 1);
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 1);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        return config;
    }

    private Integer getKey(Sentence s) {
        // return null;
        // return s.getText().length();
        // return 0;
        return s.getChapter();
    }

}
