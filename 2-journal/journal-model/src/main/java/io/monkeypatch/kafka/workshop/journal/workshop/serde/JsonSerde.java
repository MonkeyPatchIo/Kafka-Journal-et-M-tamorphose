package io.monkeypatch.kafka.workshop.journal.workshop.serde;

public class JsonSerde {

    public static class IntegerSerde extends BaseJsonSerde<Integer> {
        public IntegerSerde() { super(Integer.class); }
    }
    public static class StringSerde extends BaseJsonSerde<String> {
        public StringSerde() { super(String.class); }
    }
}
