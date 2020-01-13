package io.monkeypatch.kafka.workshop.journal;

import java.util.UUID;

class AbstractExercise {

    public static final String brokers = "localhost:9092";

    public String topicName() {
        return String.format("%s-%s",
                this.getClass().getSimpleName(),
                UUID.randomUUID().toString());
    }

}
