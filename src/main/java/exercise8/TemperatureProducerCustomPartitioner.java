package exercise8;

import exercise7.model.Location;
import exercise7.model.LocationType;
import exercise7.model.Observation;
import exercise7.model.ObservationType;
import exercise7.serialization.LocationSerializer;
import exercise7.serialization.ObservationSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class TemperatureProducerCustomPartitioner {
    public void createProducer() {

        Random random = new Random(1);

        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LocationSerializer.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ObservationSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, LocationPartitioner.class.getName());

        // TODO: Create a KafkaProducer

        try (KafkaProducer<Location, Observation> termometer = new KafkaProducer<>(props)) {

            while (true) {

                Location key = new Location("room" + random.nextInt(5), LocationType.ROOM);

                int temperature = random.nextInt(40);

                if (random.nextBoolean())
                    temperature = -1 * temperature;

                Observation value = new Observation(temperature, System.currentTimeMillis(), ObservationType.TEMPERATURE);

                ProducerRecord<Location, Observation> record =
                        new ProducerRecord<>("temperature", key, value);

                termometer.send(record);

                Thread.sleep(10001);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        TemperatureProducerCustomPartitioner helloProducer = new TemperatureProducerCustomPartitioner();
        helloProducer.createProducer();
    }
}
