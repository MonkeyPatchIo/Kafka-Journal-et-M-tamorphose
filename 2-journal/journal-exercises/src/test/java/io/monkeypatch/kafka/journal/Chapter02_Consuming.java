package io.monkeypatch.kafka.journal;

import io.monkeypatch.kafka.journal.utils.KakfaBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This chapter provides an introduction to the other side of the coin:
 * consuming the data we previously created.
 */
public class Chapter02_Consuming extends KakfaBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter02_Consuming.class);

    String sourceTopic = topicName();
    Supplier<Stream<Sentence>> sentences = () -> Sentence.firstNSentencesByChapter(20);
    Integer sentenceCount = io.vavr.collection.Stream.ofAll(sentences.get()).map(s -> 1).sum().intValue();

    @BeforeEach
    void produceContent() {
        produceToTopic(sourceTopic, sentences.get(), this::getKey);
    }

    private Integer getKey(Sentence s) {
        // return null;
        // return s.getText().length();
        // return 0;
        return s.getChapter();
    }

    @Test
    void testConsumer () {
        // The test will be simple: just increment this number for each received message.
        // If the number of received messages matches the number of messages in the topic
        // (that is, the number of sentences in Kafka's books), then the test succeeds.
        int countReceived = 0;

        // First, create a consumer config...
        Properties config = consumerConfig();

        // Next, we instantiate the consumer.
        Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);

        // Now, we need to tell Kafka what to consume.
        // Notice that we give a list of topics. We listen to only one here, but we could
        // subscribe to a regexp pattern matching on topic names, or even be very
        // fine-grained and select specific partitions by using the Consumer#assign method.
        consumer.subscribe(List.of(sourceTopic));

        try {
            // Now, we are ready to consume.
            // Consuming with a KafkaConsumer is an entirely synchronous operation.
            // Once you start to work with a consumer in a given thread, you can only
            // manipulate it in the same thread.
            while (countReceived < sentenceCount) {
                // Tell the consumer to listen to messages for at most 1 seconds.
                // Most of the time, it will return faster than that, only waiting
                // that amount of time when there is nothin to consume. Some
                // consumer config parameters can tune this behaviour: wait for at most
                // a given number of messages, a max number of bytes received, etc.
                ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofSeconds(10));
                LOG.info("CONSUMER Polled {} records", records.count());

                for (ConsumerRecord<Integer, Sentence> record : records) {
                    // Just so that you can see what each partition contained,
                    // we will create a folder for the dest topic, and a file
                    // for each partition in this folder. The files will contain
                    // the offset/keys/values for each record corresponding to this partition.
                    registerRecordInPartitionFiles(record, "target/ch02/");
                    LOG.info("CONSUMED {} key={} msg={}", msgId(record), record.key(), record.value());
                    countReceived += 1;
                }
            }

            assertThat(countReceived).isEqualTo(sentenceCount);
        }
        finally {
            // Don't forget to close the consumers when you're done with them.
            consumer.close();
        }
    }

    // Creating a consumer config needs more work than for a producer.
    // Here, we'll be setting some more properties.
    private Properties consumerConfig() {
        int rand = random.nextInt();
        Properties config = new Properties();

        // Where to contact the kafka brokers, always required...
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        // Deserializers for keys and values, pretty much needed also.
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        // This tells which group the consumer will be in. Required, even if
        // our consumer will be the only one in its group for now.
        // Here, we create a new group for each test run...
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + rand);
        // This tells the broker what to do when this consumer appears for the
        // first time. We tell the broker to send messages from the beginning of
        // the topic (or what is left of it, because some messages may have been
        // removed to make more room for others).
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // We do not care about committing manually the reading of messages
        // so the consumer commits by itself when it feels appropriate to do so.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // We can limit the number of messages on each poll, if they are coming too fast.
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // Because we run on local clusters and with very few data,
        // we lower the buffer sizes and relax on isolation levels.
        // This prevents some erratic cases where consumer never receive data.
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "1");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, "1");

        return config;
    }

}
