package inclass;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Properties;

public class MyKafkaStreams extends KafkaStreams {
    public MyKafkaStreams(Topology topology, Properties props) {
        super(topology, props);
    }

    public MyKafkaStreams(Topology topology, Properties props, KafkaClientSupplier clientSupplier) {
        super(topology, props, clientSupplier);
    }

    public MyKafkaStreams(Topology topology, Properties props, Time time) {
        super(topology, props, time);
    }

    public MyKafkaStreams(Topology topology, Properties props, KafkaClientSupplier clientSupplier, Time time) {
        super(topology, props, clientSupplier, time);
    }

    public MyKafkaStreams(Topology topology, StreamsConfig config) {
        super(topology, config);
    }

    public MyKafkaStreams(Topology topology, StreamsConfig config, KafkaClientSupplier clientSupplier) {
        super(topology, config, clientSupplier);
    }

    public MyKafkaStreams(Topology topology, StreamsConfig config, Time time) {
        super(topology, config, time);
    }

    public StreamThread[] threads(){
        return threads;
    }
}
