package io.monkeypatch.kafka.workshop.journal;

import io.monkeypatch.kafka.workshop.model.Sentence;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.stream.Stream;

class Exercise01_Producing extends AbstractExercise {

    @Test
    void produceSentences() {

        // The very first thing to do is create a producer configuration.
        // It takes the form of a Properties instance.
        Properties config = new Properties();

        // Setting where to reach the brokers is always necessary.
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        // We set the key/value serializers.
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class);

        Producer<Integer, Sentence> producer = new KafkaProducer<>(config);

        Stream<Sentence> allSentences = Sentence.fromAllBooks();
        allSentences.forEach(sentence -> {

            // The producer is not tied to a specific topic.
            // You can choose a different topic for each sentence.
            String destTopic = topicName(sentence);

            Integer key = getKey(sentence);

            ProducerRecord<Integer, Sentence> record = new ProducerRecord<>(destTopic, key, sentence);

            producer.send(record, (recordMetadata, e) -> {
                System.out.println("Sent " + sentence + ": " + e);
            });
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
