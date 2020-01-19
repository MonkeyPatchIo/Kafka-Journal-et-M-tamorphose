package io.monkeypatch.kafka.journal;

import io.monkeypatch.kafka.journal.utils.KakfaBoilerplate;
import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class Chapter01_Producing extends KakfaBoilerplate {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter01_Producing.class);

    @Test
    void produceSentences() {
        String destTopic = topicName();
        Stream<Sentence> allSentences = Sentence.fromAllBooks();
        Map<String, Tuple2<Integer, Sentence>> producedMessages = new ConcurrentHashMap<>();

        createTopic(destTopic, partitions);

        // The very first thing to do is create a producer configuration.
        // It takes the form of a Properties instance.
        Properties config = new Properties();
        // Setting where to reach the brokers is always necessary.
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-test");
        // We set the key/value serializers.
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class);

        Producer<Integer, Sentence> producer = new KafkaProducer<>(config);
        AtomicInteger countSentences = new AtomicInteger();

        //*//
        // Now we send a kafka message for each sentence, on the destTopic.
        allSentences.forEach(sentence -> {
            LOG.info("Sending sentence {}", sentence.getId());
            countSentences.incrementAndGet();

            Integer key = getKey(sentence);
            ProducerRecord<Integer, Sentence> record = new ProducerRecord<>(destTopic, key, sentence);

            producer.send(record, (recordMetadata, e) -> {
                if (e != null) { e.printStackTrace(); return; }
                String msgId = String.format("%s-%d-%d",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset());
                LOG.info("Sent message {} for sentence {}", msgId, sentence.getId());
                producedMessages.put(msgId, Tuple.of(key, sentence));
            });
        });
        //*/

        Map<String, Tuple2<Integer, Sentence>> consumedMessages = consumeFromTopicToMap(destTopic, countSentences.get());
        consumedMessages.forEach((k,v) -> {
            assertThat(producedMessages).containsKey(k);
            if (v._1 == null) { assertThat(producedMessages.get(k)._1).isNull(); }
            else { assertThat(producedMessages.get(k)._1).isSameAs(v._1); }
            assertThat(producedMessages.get(k)._2).isEqualTo(v._2);
        });
    }

    // You can choose your own implementation for the destination topic name.
    //
    // The broker is configured to automatically create a topic with 3 partitions
    // if it does not already exist (this is the default behaviour).
    //
    // Tinker at will!
    private String topicName(Sentence s) {
        // return testRunTopic;
        return testRunTopic + s.getBook();
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
        return null;
        // return s.getText().length();
        // return 0;
        // return s.getChapter();
    }

}
