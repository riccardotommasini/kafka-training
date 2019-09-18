package exercise7.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import exercise7.model.Observation;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ObservationDeserializer implements Deserializer<Observation> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Observation deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Observation user = null;
        try {
            user = mapper.readValue(data, Observation.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }
}
