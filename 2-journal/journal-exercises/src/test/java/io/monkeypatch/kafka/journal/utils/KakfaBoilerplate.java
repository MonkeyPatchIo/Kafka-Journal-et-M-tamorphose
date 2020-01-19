package io.monkeypatch.kafka.journal.utils;

import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public void produceToTopic(
        String destTopic,
        Stream<Sentence> sentences,
        Function<Sentence, Integer> keyExtractor
    ) {
        createTopic(destTopic, partitions);
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, 1);
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 1);
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        Producer<Integer, Sentence> producer = new KafkaProducer<>(config);
        sentences.forEach(s -> {
            Integer key = keyExtractor.apply(s);
            Try.of(() -> producer.send(new ProducerRecord<>(destTopic, key, s)).get())
                .peek(md -> LOG.info("PRODUCED {} key={} msg={}", msgId(md), key, s))
                .onFailure(e -> LOG.error(e.getMessage(), e));
        });
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
        int rand = new Random().nextInt();

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + rand);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-client-" + rand);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);

        try {
            LocalTime waitUntil = LocalTime.now().plusSeconds(5);
            Map<String, Tuple2<Integer, Sentence>> result = new ConcurrentHashMap<>();

            consumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    //LOG.info("Revoked: {}", collection);
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    //LOG.info("Assigned: {}", collection);
                }
            });

            AtomicBoolean hasPolled = new AtomicBoolean();
            while (result.size() < max && waitUntil.isAfter(LocalTime.now())) {
                ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofSeconds(1));
                //LOG.info("Assigned to {} partitions", consumer.assignment().size());
                //LOG.info("Polled: {}", records.count());
                records.forEach(record -> {
                    hasPolled.set(true);
                    Tuple2<Integer, Sentence> kv = Tuple.of(record.key(), record.value());
                    String msgId = msgId(record);
                    result.put(msgId, kv);
                    LOG.info("CONSUMED SYNC  message {} for sentence {}", msgId, record.value());
                });
                //LOG.info("Total messages seen: {} (max is {})", result.size(), max);
            }
            return result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            consumer.close();
        }
    }

    public String msgId(ConsumerRecord<Integer, Sentence> record) {
        return String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
    }

    public String msgId(RecordMetadata record) {
        return String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
    }


    private Map<TopicPartition, FileWriter> files = new HashMap<>();

    protected void registerRecordInPartitionFiles(ConsumerRecord<Integer, Sentence> record) {
        try {
            String topic = record.topic();
            int partition = record.partition();
            TopicPartition tp = new TopicPartition(topic, partition);
            if (!files.containsKey(tp)) {
                File folder = new File("target/topics/" + topic);
                folder.mkdirs();
                File file = new File(folder, "/partition_" + partition + ".csv");
                String fileAbsPath = file.getAbsolutePath();
                LOG.info("File for partition {} located there: {}", tp, fileAbsPath);
                files.put(tp, new FileWriter(fileAbsPath));
            }
            Writer writer = files.get(tp);
            writer.write(String.format("%06d,%06d,%s\n", record.offset(), record.key(), record.value()));
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    protected void dumpPartitionFiles() {
        try {
            for (FileWriter w: files.values()) {
                w.flush();
                w.close();
            }
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


}
