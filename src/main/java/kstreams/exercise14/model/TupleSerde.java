package kstreams.exercise14.model;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TupleSerde<L, M> implements Serde<Tuple<L, M>> {

    private TupleDeserializer<L,M> deserializer = new TupleDeserializer<L,M>();
    private TupleSerializer<L,M> serializer = new TupleSerializer<L,M>();

    @Override
    public Serializer<Tuple<L, M>> serializer() {
        return (topic, data) -> serializer.serialize(topic, data);
    }

    @Override
    public Deserializer<Tuple<L, M>> deserializer() {
        return (topic, data) -> deserializer.deserialize(topic, data);
    }
}
