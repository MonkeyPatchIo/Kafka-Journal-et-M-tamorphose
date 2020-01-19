package io.monkeypatch.kafka.journal;

import io.monkeypatch.kafka.journal.utils.KakfaBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class Chapter01_Producing extends KakfaBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter01_Producing.class);

    // This is the name of topic we will be sending message to.
    // Each execution of the test has its own topic.
    String destTopic = topicName();

    @BeforeEach
    void ensureTopic() {
        // Here, we ensure our destination topic is created,
        // with the desired number of partitions.
        createTopic(destTopic, partitions);
    }

    @Test
    void produceSentences() {
        // The very first thing to do is create a producer configuration.
        // It takes the form of a Properties instance.
        Properties config = createProducerConfig();
        // Then, we can instantiate our producer with the configuration.
        Producer<Integer, Sentence> producer = new KafkaProducer<>(config);

        // We load the messages to be sent to the topic.
        Stream<Sentence> allSentences = Sentence.fromAllBooks();

        // This container allows us to keep track of the sent messages
        Map<String, Tuple2<Integer, Sentence>> producedMessages = new ConcurrentHashMap<>();

        // Now we send a kafka message for each sentence, on the destTopic.
        allSentences.forEach(sentence -> {
            // We create the message key from the sentence.
            Integer key = getKey(sentence);
            // And create a record, with the destination topic, message key and message value.
            ProducerRecord<Integer, Sentence> record = new ProducerRecord<>(destTopic, key, sentence);
            // Now we only have to send the record. This is an asynchronous operation by default.
            Future<RecordMetadata> sent = producer.send(
                record,
                // This optional callback is run once the record has been sent, or an error occurred.
                // It is executed in another thread, though.
                (recordMetadata, e) -> {
                    if (e != null) {
                        LOG.error("An error occurred while sending {}", record, e);
                        return;
                    }
                    String msgId = msgId(recordMetadata);
                    //LOG.info("PRODUCED ASYNC message {} for sentence {}", msgId, sentence.getId());
                    // And we keep track of the sent message.
                    producedMessages.put(msgId, Tuple.of(key, sentence));
                });

            // We can wait make the sending operation synchronous by waiting for the
            // future to complete.
            Try.of(() -> sent.get())
                .onFailure(t -> LOG.error(t.getMessage(),  t))
                .forEach(recordMetadata -> {
                    String msgId = msgId(recordMetadata);
                    LOG.info("PRODUCED SYNC  message {} for sentence {}", msgId, sentence);
                });
        });

        assertThatAllProducedIsConsumedBack(destTopic, producedMessages);
    }

    // Creating a well adapted producer configuration can be tricky.
    // For this test, however, we keep things minimal and simple.
    // Only the strictly necessary parameters are set.
    private Properties createProducerConfig() {
        Properties config = new Properties();
        // Setting where to reach the brokers is always necessary.
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        // We set the key/value serializers.
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        return config;
    }


    // We consume from our destination topic and ensure that everything that was produced is there.
    // We'll see the consumption mechanics in another chapter, so don't worry about this code for now.
    private void assertThatAllProducedIsConsumedBack(String destTopic, Map<String, Tuple2<Integer, Sentence>> producedMessages) {
        Map<String, Tuple2<Integer, Sentence>> consumedMessages =
            consumeFromTopicToMap(destTopic, producedMessages.size());
        consumedMessages.forEach((k,v) -> {
            assertThat(producedMessages).containsKey(k);
            if (v._1 == null) { assertThat(producedMessages.get(k)._1).isNull(); }
            else { assertThat(producedMessages.get(k)._1).isSameAs(v._1); }
            assertThat(producedMessages.get(k)._2).isEqualTo(v._2);
        });
    }

    // You can choose your own implementation for the message key.
    //
    // Since we're using the default partition attribution mechanics,
    // the key will determine which partition the message is sent to.
    //
    // If the key is null, we're telling Kafka that we don't care
    // which partition the message is sent to.
    //
    // Tinker at will!
    private Integer getKey(Sentence s) {
        // return null;
        // return s.getText().length();
        // return 0;
        return s.getChapter();
    }

}
