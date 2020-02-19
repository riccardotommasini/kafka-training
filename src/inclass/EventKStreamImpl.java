package inclass;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.internals.AbstractStream;
import org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;

import java.util.Set;

public class EventKStreamImpl<K,V> extends AbstractStream<K, V> {


    public EventKStreamImpl(AbstractStream<K, V> stream) {
        super(stream);
    }


}
