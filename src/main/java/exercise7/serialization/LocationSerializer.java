package exercise7.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import exercise7.model.Location;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class LocationSerializer implements Serializer<Location> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Location data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
