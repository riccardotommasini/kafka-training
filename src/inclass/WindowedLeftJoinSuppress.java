package inclass;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Properties;

public class WindowedLeftJoinSuppress {

    static Gson gson = new Gson();

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "window-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        KStream<String, Event> As = builder.stream("As", Consumed.with(Serdes.String(), new EventSerde()));
        KStream<String, Event> Bs = builder.stream("Bs", Consumed.with(Serdes.String(), new EventSerde()));


        As.join(Bs, (a, b) -> {
            if (a.timestamp() > b.timestamp())
                return a;
            else
                return null;

        }, JoinWindows.of(10).before());


        Topology topology = builder.build();

        System.out.println(topology.describe().toString());

        KafkaStreams ks = new KafkaStreams(topology, props);
        ks.start();
    }

    //[KSTREAM-SOURCE-0000000000]: 1013, {"ordertime":1509049597997,"orderid":1013,"itemid":"Item_273","orderunits":5.076322735052166,"address":{"city":"City_14","state":"State_31","zipcode":80962}}

}



