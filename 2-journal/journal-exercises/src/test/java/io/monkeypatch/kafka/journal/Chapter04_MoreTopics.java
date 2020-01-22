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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In this chapter, we run a producer which send messages to several topics.
 *
 * We use a single consumer to consume from all of them at the same time.
 *
 * Topics are nothing but convenient ways to refer to groups of partitions.
 */
public class Chapter04_MoreTopics extends KakfaBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter04_MoreTopics.class);

    String sourceTopicBase = topicName();
    Supplier<Stream<Sentence>> sentences = () -> Sentence.fromAllBooks();
    Integer sentenceCount = io.vavr.collection.Stream.ofAll(sentences.get()).map(s -> 1).sum().intValue();

    // In this chapter, we create several topics.
    // Here, we provide a function in order to determine the suffix of the topic
    // for a given sentence.
    public String topicSuffix(Sentence s) {
        //  Topics organised by book:
        //return s.getBook();
        //   Topics organised by book/chapter:
        //return s.getBook() + s.getChapter();
        //   Topics organised by first letter in sentence:
        return ("" + s.getText().toLowerCase().charAt(0)).replaceFirst("\\W", "_");
    }
    public String topicForSentence(Sentence s) {
        return sourceTopicBase + "-" + topicSuffix(s);
    }

    @BeforeEach
    void initTopics() {
        // Creating the topics upfront
        createTopics(sentences.get().map(this::topicForSentence).collect(Collectors.toSet()), 3);
    }

    @Test
    void testPatternConsuming() throws Exception {
        int groupdSuffix = random.nextInt();
        String groupId = String.format("test-group-%d", groupdSuffix);
        Properties config = consumerConfig(groupId);

        CountDownLatch latch = new CountDownLatch(sentenceCount);
        AtomicBoolean finished = new AtomicBoolean();

        // Run two consumers (in the same group) in parallel, forcing them to share partitions.
        Pattern topicPattern = Pattern.compile(sourceTopicBase + "-.*");
        runConsumer(topicPattern, config, "consumer-1", latch, finished);
        runConsumer(topicPattern, config, "consumer-2", latch, finished);

        // Start producing to the topics asynchronously, choosing a specific topic for each sentence
        runProducer(this::topicForSentence, sentences.get(), this::getKey, true);

        assertThat(latch.await(2, TimeUnit.MINUTES)).isTrue();
        finished.set(true);
    }

    private void runConsumer(Pattern topicPattern, Properties baseConfig, String consumerId, CountDownLatch latch, AtomicBoolean finished) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            Properties config = (Properties)baseConfig.clone();
            Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);
            try {
                // We consume from a pattern of topics
                consumer.subscribe(topicPattern);
                do {
                    ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofMillis(500));
                    LOG.info("CONSUMER {} polled {} records", consumerId, records.count());
                    for (ConsumerRecord<Integer, Sentence> record : records) {
                        registerRecordInPartitionFiles(record, "target/ch04/" + consumerId + "/");
                        latch.countDown();
                        long count = latch.getCount();
                        if(count % 10 == 0) LOG.info("CONSUMER {} latch count at {}", consumerId, count);
                    }
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
