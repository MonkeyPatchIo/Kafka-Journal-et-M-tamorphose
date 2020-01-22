package io.monkeypatch.kafka.journal;

import io.monkeypatch.kafka.journal.utils.KakfaBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The kafka brokers keep track of the last offset polled, for each group, for each partition.
 *
 * By default, the consumers have a enable.auto.commit value to true.
 *
 * In our tests until now, this value has been set to false.
 *
 * In this chapter, we attempt to manually commit this information.
 */
public class Chapter06_ReturningConsumer extends KakfaBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter06_ReturningConsumer.class);

    String sourceTopic = topicName();
    Supplier<Stream<Sentence>> sentences = () -> Sentence.fromAllBooks();
    Integer sentenceCount = io.vavr.collection.Stream.ofAll(sentences.get()).map(s -> 1).sum().intValue();

    @BeforeEach void initTopic() {
        createTopic(sourceTopic, 10);
    }

    @Test
    void testReturningConsumer() throws Exception {
        int groupSuffix = random.nextInt();
        CountDownLatch latch = new CountDownLatch(sentenceCount);

        // Our consumer is instructed to consume only a fixed amount of messages before closing itself.
        // We will loop and create a new consumer each time the previous one has reached this max number of messages.
        int maxNumberOfMessagesConsumedByEachConsumer = 100;

        runProducer(s -> sourceTopic, sentences.get(), this::getKey, true);

        int iteration = 0; // This number allows to separate the dumped files in folders per iteration
        do {

            // What happens if we give a new groupId each time instead?
            String groupId = String.format("test-group-%d", groupSuffix);

            Properties config = consumerConfig(groupId);

            CountDownLatch finished = new CountDownLatch(1);
            runConsumer(sourceTopic, config, "consumer-1", latch, finished,
                    iteration,
                    maxNumberOfMessagesConsumedByEachConsumer
            );
            finished.await(30, TimeUnit.SECONDS);
            iteration += 1;
        } while(latch.getCount() > 0);

    }

    private void runConsumer(String sourceTopic, Properties baseConfig, String consumerId,
                             CountDownLatch latch,
                             CountDownLatch finished,
                             int iteration,
                             int pollMaxMessages
    ) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            Properties config = (Properties)baseConfig.clone();
            Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);
            int polled = 0;
            try {
                consumer.subscribe(List.of(sourceTopic));
                do {
                    ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofMillis(500));
                    LOG.info("CONSUMER {} polled {} records", consumerId, records.count());
                    for (ConsumerRecord<Integer, Sentence> record : records) {
                        polled += 1;
                        registerRecordInPartitionFiles(record, "target/ch06/iteration_" + iteration + "/");
                        latch.countDown();
                    }

                    // What happens if we do not commit here?
                    consumer.commitSync(Duration.ofSeconds(3));

                } while (polled < pollMaxMessages && latch.getCount() > 0);
                finished.countDown();
            }
            catch(Exception e) {
                LOG.error(e.getMessage(), e);
            }
            finally {
                consumer.close();
                executor.shutdown();
            }
        });
    }

    private Properties consumerConfig(String groupId) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Sentence.Serde.class);

        // What happens if we give a different group id each time?
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // What happens if we set auto commit to true, and do not commit in the consumer loop?
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
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
        return s.getText().length();
        // return 0;
        // return s.getChapter();
    }


}
