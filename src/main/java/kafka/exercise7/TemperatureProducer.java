package kafka.exercise7;

import kafka.exercise7.model.Location;
import kafka.exercise7.model.LocationType;
import kafka.exercise7.model.Observation;
import kafka.exercise7.model.ObservationType;
import kafka.exercise7.serialization.LocationSerializer;
import kafka.exercise7.serialization.ObservationSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class TemperatureProducer {
    public void createProducer() {

        Random random = new Random(1);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LocationSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ObservationSerializer.class.getName());

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

                Thread.sleep(10000);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        TemperatureProducer helloProducer = new TemperatureProducer();
        helloProducer.createProducer();
    }
}
