package io.monkeypatch.kafka.workshop.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.kafka.workshop.serde.BaseJsonSerde;

public class Sentence {

    public static class Serde extends BaseJsonSerde<Sentence> {
        public Serde() { super(Sentence.class); }
    }

    @JsonProperty("id") private final int id;
    @JsonProperty("b") private final String book;
    @JsonProperty("c") private final int chapter;
    @JsonProperty("t") private final String text;

    @JsonCreator
    public Sentence(
            @JsonProperty("id") int id,
            @JsonProperty("b") String book,
            @JsonProperty("c") int chapter,
            @JsonProperty("t") String text) {
        this.id = id;
        this.book = book;
        this.chapter = chapter;
        this.text = text;
    }

    public int getId() {
        return id;
    }

    public String getBook() {
        return book;
    }

    public int getChapter() {
        return chapter;
    }

    public String getText() {
        return text;
    }

}
