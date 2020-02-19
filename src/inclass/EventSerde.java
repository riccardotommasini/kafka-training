package inclass;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class EventSerde implements Serde<Event> {
    @Override
    public Serializer<Event> serializer() {
        return null;
    }

    @Override
    public Deserializer<Event> deserializer() {
        return null;
    }
}
