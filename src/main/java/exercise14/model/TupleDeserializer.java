package exercise14.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class TupleDeserializer<L,M> implements Deserializer<Tuple<L,M>> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Tuple deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Tuple t = null;
        try {
            t = mapper.readValue(data, Tuple.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return t;
    }

    @Override
    public void close() {

    }
}
