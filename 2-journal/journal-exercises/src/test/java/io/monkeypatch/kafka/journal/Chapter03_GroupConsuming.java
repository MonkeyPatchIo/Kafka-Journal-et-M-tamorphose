package io.monkeypatch.kafka.journal;

import io.monkeypatch.kafka.journal.utils.KakfaBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class Chapter03_GroupConsuming extends KakfaBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter03_GroupConsuming.class);

    String sourceTopic = topicName();
    Supplier<Stream<Sentence>> sentences = () -> Sentence.fromAllBooks();
    Integer sentenceCount = io.vavr.collection.Stream.ofAll(sentences.get()).map(s -> 1).sum().intValue();

    @BeforeEach void initTopic() {
        createTopic(sourceTopic, 10);
    }

    @Test
    void testGroupConsuming() throws Exception {
        int rand = random.nextInt();
        String groupId = String.format("test-group-%d", rand);
        Properties config = consumerConfig(groupId);

        CountDownLatch latch = new CountDownLatch(sentenceCount);
        AtomicBoolean finished = new AtomicBoolean();
        AtomicBoolean consumer1Finished = new AtomicBoolean();
        AtomicInteger throttlePollingMs = new AtomicInteger(1000);

        // Run two consumers in parallel, forcing them to share partitions.
        runConsumer(config, "consumer-1", latch, consumer1Finished, throttlePollingMs);
        runConsumer(config, "consumer-2", latch, finished, throttlePollingMs);

        // Start producing to the topic asynchronously
        Executors.newSingleThreadExecutor().submit(() ->
                produceToTopic(sourceTopic, sentences.get(), this::getKey, false, false)
        );

        // Wait for the 2 first consumers to start receiving messages
        Try.run(() -> Thread.sleep(5000));

        // Add a new consumer, provoking rebalancing of partitions between consumers.
        runConsumer(config, "consumer-3", latch, finished, throttlePollingMs);
        // Wait for rebalancing to happen...
        Try.run(() -> Thread.sleep(5000));
        // Stopping the consumer1, leaving only 2 and 3 to do the work,
        // forcing another rebalance.
        consumer1Finished.set(true);
        throttlePollingMs.set(0); // No need to make things longer anymore...

        // We're happy with reaching this point quickly enough, no tests beyond that.
        assertThat(latch.await(2, TimeUnit.MINUTES)).isTrue();

        // Allow consumers to terminate properly.
        finished.set(true);

        // You can look at the partition files in this project's target/ch03 folder.
        dumpPartitionFiles();
    }

    private void runConsumer(Properties config, String consumerId, CountDownLatch latch, AtomicBoolean finished, AtomicInteger throttlePollingMs) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);
            try {
                consumer.subscribe(
                        List.of(sourceTopic),
                        // This optional listener will detect when the consumer is assigned
                        // partitions, and when they are revoked.
                        new ConsumerRebalanceListener() {
                            @Override
                            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                                partitions.forEach(p -> LOG.info("CONSUMER {} revoked : {}-{}", consumerId, p.topic(), p.partition()));
                            }

                            @Override
                            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                                partitions.forEach(p -> LOG.info("CONSUMER {} assigned: {}-{}", consumerId, p.topic(), p.partition()));
                            }
                        }
                );
                do {
                    ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofMillis(500));
                    LOG.info("CONSUMER {} polled {} records", consumerId, records.count());
                    for (ConsumerRecord<Integer, Sentence> record : records) {
                        registerRecordInPartitionFiles(record, "target/ch03/" + consumerId + "/");
                        latch.countDown();
                        long count = latch.getCount();
                        if(count % 10 == 0) LOG.info("CONSUMER {} latch count at {}", consumerId, count);
                    }
                    consumer.commitSync();
                    // Throttling polls during the first part of the test.
                    Try.run(() -> Thread.sleep(throttlePollingMs.get()));
                } while (!finished.get());
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
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "1");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, "1");
        return config;
    }

    private Integer getKey(Sentence s) {
        // return null;
        // return s.getText().length();
        // return 0;
        // return s.getChapter();

        // For this chapter, having a lot of keys means probably using all the partitions.
        // Other options can be tested anyway, though.
        return s.hashCode();
    }

}
