package io.monkeypatch.kafka.workshop.serde;

import io.monkeypatch.kafka.workshop.model.Sentence;

public class JsonSerde {

    public static class IntegerSerde extends BaseJsonSerde<Integer> {
        public IntegerSerde() { super(Integer.class); }
    }
    public static class StringSerde extends BaseJsonSerde<String> {
        public StringSerde() { super(String.class); }
    }
    public static class SentenceSerde extends BaseJsonSerde<Sentence> {
        public SentenceSerde() { super(Sentence.class); }
    }
}
