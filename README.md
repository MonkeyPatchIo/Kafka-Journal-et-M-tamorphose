# Kafka: Journal et Métamorphose

## 0. Setup

Copy archives from the USB key that is being shared by the coaches.  
You should find:

    $ kafka_2.12-2.3.0.tgz
    $ openjdk-11.0.2_linux-x64_bin.tar.gz
    $ openjdk-11.0.2_osx-x64_bin.tar.gz
    $ openjdk-11.0.2_windows-x64_bin.zip

Copy `kafka_2.12-2.3.0.tgz` and the JDK corresponding to your platform.

Goto `0-setup`.

TODO Setup JDK.

Setup Apache Kafka (TODO add JAVA_HOME in the mix, split windows vs others):

    $ #1. Un-tar it
    $ tar -xzf kafka_2.12-2.3.0.tgz
    $
    $ #2. Start Zookeeper
    $ bin/zookeeper-server-start.sh config/zookeeper.properties
    $
    $ #3. Start Kafka
    $ bin/kafka-server-start.sh config/server.properties
    $
    $ #4. Create a topic
    $ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
    $
    $ #5. List topics
    $ bin/kafka-topics.sh --list --bootstrap-server localhost:9092
    $
    $ #6. Send messages
    $ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
    $
    $ #7. Read messages
    $ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

## 1. Trial (aka. The presentation)

Show must ~~go on~~ begin.

## 2. Journal (aka. Apache Kafka exercises)

## 3. Métamorphose (aka. Kafka Streams exercises)
