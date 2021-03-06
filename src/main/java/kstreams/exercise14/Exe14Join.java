package kstreams.exercise14;

import kafka.advanced.exercise6.solution.exercise6a.model.Room;
import kafka.advanced.exercise6.solution.exercise6a.model.Temperature;
import kstreams.exercise12.serdes.RoomSerde;
import kstreams.exercise12.serdes.TemperatureSerde;
import kstreams.exercise14.model.ConfigSerde;
import kstreams.exercise14.model.Configuration;
import kstreams.exercise14.model.RichTemperature;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class Exe14Join {

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-average-kafkastream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TemperatureSerde.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, RoomSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStream<Room, Temperature> measures =
                builder.stream("temperature",
                        Consumed.with(new RoomSerde(), new TemperatureSerde()));

        KTable<Room, Configuration> configs = builder.table("configurations",
                Consumed.with(new RoomSerde(), new ConfigSerde()));

        measures.join(configs, RichTemperature::new)
                .filter((key, value) -> value.getValue().getValue() != value.getConfiguration().getPrefVal())
                .print(Printed.toSysOut());


        Topology topology = builder.build();

        System.out.println(topology.describe());
        KafkaStreams ks = new KafkaStreams(topology, props);
        ks.start();
    }
}
