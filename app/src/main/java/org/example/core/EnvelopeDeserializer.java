package org.example.core;

public class EnvelopeDeserializer extends JsonDeserializer<Envelope> {
    public EnvelopeDeserializer() {
        super(Envelope.class);
    }
}
