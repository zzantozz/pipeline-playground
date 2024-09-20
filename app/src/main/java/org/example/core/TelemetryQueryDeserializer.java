package org.example.core;

public class TelemetryQueryDeserializer extends JsonDeserializer<TelemetryQuery> {
    public TelemetryQueryDeserializer() {
        super(TelemetryQuery.class);
    }
}
