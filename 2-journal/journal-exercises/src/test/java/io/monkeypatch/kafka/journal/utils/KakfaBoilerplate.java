package io.monkeypatch.kafka.journal.utils;

import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KakfaBoilerplate {
    private static final Logger LOG = LoggerFactory.getLogger(KakfaBoilerplate.class);

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("HHmmssSSS");
    public static final String brokers = "localhost:9192,localhost:9292";
    public static final Integer partitions = 10;

    protected final String testRunTopic = topicName();

    public String topicName() {
        return String.format("%s-%s",
            this.getClass().getSimpleName(),
            //"test1"
            dateFormat.format(new Date())
        );
    }

    public void createTopic(String topicName, int partitions) {
        try {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            AdminClient admin = KafkaAdminClient.create(config);
            CreateTopicsResult result = admin.createTopics(List.of(new NewTopic(topicName, partitions, (short) 2)));
            // Wait for the topic to be created
            result.all().get();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    protected Map<String, Tuple2<Integer, Sentence>> consumeFromTopicToMap(String topicName, int max) {
        LOG.info("Polling start...");
        Map<String, Tuple2<Integer, Sentence>> result = pollForFiveSeconds(topicName, max);
        for (int i = 0; i < 10; i++) {
            if (result.size() == max) {
                return result;
            } else {
                // Try again...
                result = pollForFiveSeconds(topicName, max);
            }
        }
        return result;
    }

    private Map<String, Tuple2<Integer, Sentence>> pollForFiveSeconds(String topicName, int max) {
        try {

            /*
            Properties adminConfig = new Properties();
            adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            List<TopicPartition> partitions = AdminClient.create(adminConfig).describeTopics(List.of(topicName))
                    .all().get().values()
                    .stream()
                    .flatMap(td -> td.partitions().stream()
                            .map(tpi -> new TopicPartition(td.name(), tpi.partition())))
                    .collect(Collectors.toList());
            */

            int rand = new Random().nextInt();

            Properties config = new Properties();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + rand);
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-client-" + rand);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

            LocalTime waitUntil = LocalTime.now().plusSeconds(5);
            Map<String, Tuple2<Integer, Sentence>> result = new ConcurrentHashMap<>();
            Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);

            //consumer.assign(partitions);
            consumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    LOG.info("Revoked: {}", collection);
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    LOG.info("Assigned: {}", collection);
                }
            });

            AtomicBoolean hasPolled = new AtomicBoolean();
            while (result.size() < max && waitUntil.isAfter(LocalTime.now())) {
                ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofSeconds(1));
                LOG.info("Assigned to {} partitions", consumer.assignment().size());
                LOG.info("Polled: {}", records.count());
                records.forEach(record -> {
                    hasPolled.set(true);
                    Tuple2<Integer, Sentence> kv = Tuple.of(record.key(), record.value());
                    String msgId = String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
                    result.put(msgId, kv);
                });
                LOG.info("Total messages seen: {} (max is {})", result.size(), max);
            }
            if (!hasPolled.get()) {
                consumer.close();
            }
            return result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
