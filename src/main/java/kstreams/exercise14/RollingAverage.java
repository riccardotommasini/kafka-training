package kstreams.exercise14;

import kstreams.exercise14.model.Tuple;
import kstreams.exercise14.model.TupleSerde;
import kafka.exercise7.model.Location;
import kafka.exercise7.model.Observation;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class RollingAverage {

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rollingagg11");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LocationSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ObservationSerde.class);

        LocationSerde keySerde = new LocationSerde();
        KStream<Location, Observation> observations = builder
                .stream("temperature", Consumed.with(keySerde, new ObservationSerde()));


        KStream<Location, Tuple<Integer,Integer>> tuples = observations.mapValues(value -> new Tuple<>(0, new Double(value.getValue()).intValue()))
                .groupByKey().aggregate(() -> new Tuple<>(0, 0),
                        (key, value, agg) -> {
                            agg.t1++;
                            agg.t2 += value.t2;
                            return agg;
                        }, Materialized.with(keySerde, new TupleSerde<>())).toStream();
//
        tuples.mapValues(value -> value.t2 / value.t1).print(Printed.toSysOut());

        Topology topology = builder.build();
        KafkaStreams ks = new KafkaStreams(topology, props);
        ks.start();
    }
}


//todo calculate stream rate per room