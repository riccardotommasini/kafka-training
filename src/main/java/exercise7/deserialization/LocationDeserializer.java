package exercise7.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import exercise7.model.Location;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class LocationDeserializer implements Deserializer<Location> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Location deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Location user = null;
        try {
            user = mapper.readValue(data, Location.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }
}
