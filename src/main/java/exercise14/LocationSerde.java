package exercise14;

import exercise7.deserialization.LocationDeserializer;
import exercise7.model.Location;
import exercise7.model.Observation;
import exercise7.serialization.LocationSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class LocationSerde implements Serde<Location> {

    LocationSerializer serializer = new LocationSerializer();
    LocationDeserializer deserializer = new LocationDeserializer();

    @Override
    public Serializer<Location> serializer() {
        return (topic, data) -> serializer.serialize(topic, data);
    }

    @Override
    public Deserializer<Location> deserializer() {
        return (topic, data) -> deserializer.deserialize(topic, data);
    }
}
