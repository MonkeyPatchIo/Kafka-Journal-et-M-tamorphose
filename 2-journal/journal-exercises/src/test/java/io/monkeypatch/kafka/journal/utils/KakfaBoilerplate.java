package io.monkeypatch.kafka.journal.utils;

import io.monkeypatch.kafka.workshop.model.Sentence;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KakfaBoilerplate {
    private static final Logger LOG = LoggerFactory.getLogger(KakfaBoilerplate.class);

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("HHmmssSSS");
    public static final String brokers = "localhost:9092";
    public static final Integer partitions = 10;

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
        produceToTopic(destTopic, sentences, keyExtractor, false, true);
    }

    public void produceToTopic(
            String destTopic,
            Stream<Sentence> sentences,
            Function<Sentence, Integer> keyExtractor,
            boolean silent,
            boolean createTopic
    ) {
        if (createTopic) { createTopic(destTopic, partitions); }
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, 1);
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 1);
        config.put(ProducerConfig.ACKS_CONFIG, "1");

        Producer<Integer, Sentence> producer = new KafkaProducer<>(config);
        sentences.forEach(s -> {
            Integer key = keyExtractor.apply(s);
            Try.of(() -> producer.send(new ProducerRecord<>(destTopic, key, s)).get())
                .peek(md -> { if(!silent) { LOG.info("PRODUCED {} key={} msg={}", msgId(md), key, s); }})
                .onFailure(e -> LOG.error(e.getMessage(), e));
        });
    }

    public void createTopic(String topicName, int partitions) {
        createTopics(Set.of(topicName), partitions);
    }

    public void createTopics(Set<String> topicNames, int partitions) {
        try {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            AdminClient admin = KafkaAdminClient.create(config);
            List<NewTopic> newTopics = topicNames.stream()
                    .map(name -> new NewTopic(name, partitions, (short) 1))
                    .collect(Collectors.toList());
            admin.createTopics(newTopics).all().get();
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
        String baseFolder = "target/ch01";
        files.clear();
        Try.run(() -> FileUtils.forceDelete(new File(baseFolder)));

        int rand = new Random().nextInt();

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Sentence.Serde.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + rand);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Consumer<Integer, Sentence> consumer = new KafkaConsumer<>(config);

        try {
            LocalTime waitUntil = LocalTime.now().plusSeconds(5);
            Map<String, Tuple2<Integer, Sentence>> result = new ConcurrentHashMap<>();

            consumer.subscribe(List.of(topicName));

            AtomicBoolean hasPolled = new AtomicBoolean();
            while (result.size() < max && waitUntil.isAfter(LocalTime.now())) {
                ConsumerRecords<Integer, Sentence> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    hasPolled.set(true);
                    registerRecordInPartitionFiles(record, baseFolder);
                    Tuple2<Integer, Sentence> kv = Tuple.of(record.key(), record.value());
                    String msgId = msgId(record);
                    result.put(msgId, kv);
                    LOG.info("CONSUMED SYNC  message {} for sentence {}", msgId, record.value());
                });
            }
            return result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            consumer.close();
            dumpPartitionFiles();
        }
    }

    public String msgId(ConsumerRecord<Integer, Sentence> record) {
        return String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
    }

    public String msgId(RecordMetadata record) {
        return String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
    }


    private Map<Tuple2<String, TopicPartition>, FileWriter> files = new ConcurrentHashMap<>();

    protected void registerRecordInPartitionFiles(ConsumerRecord<Integer, Sentence> record, String baseFolder) {
        try {
            String topic = record.topic();
            int partition = record.partition();
            TopicPartition tp = new TopicPartition(topic, partition);
            Tuple2<String, TopicPartition> key = Tuple.of(baseFolder, tp);
            if (!files.containsKey(key)) {
                File folder = new File(baseFolder, topic);
                folder.mkdirs();
                File file = new File(folder, "/" + partition + ".json");
                FileUtils.touch(file);
                String fileAbsPath = file.getAbsolutePath();
                LOG.info("File for partition {} located there: {}", tp, fileAbsPath);
                FileWriter v = new FileWriter(fileAbsPath);
                v.write("[\n");
                files.put(key, v);
            }
            Writer writer = files.get(key);
            writer.write(String.format("  { \"offset\": %s, \"key\": %s, \"value\": %s },\n",
                    StringUtils.leftPad(""+record.offset(), 4),
                    StringUtils.leftPad(""+record.key(), 12),
                    record.value()));
            writer.flush();
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    protected void dumpPartitionFiles() {
        try {
            for (FileWriter w: files.values()) {
                w.write("]");
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
