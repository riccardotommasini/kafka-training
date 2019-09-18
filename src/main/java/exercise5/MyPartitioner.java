package exercise5;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int size = cluster.partitionsForTopic(topic).size();
        Integer number = (Integer) value;
        return partitionInfos.get(number % size).partition();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {


    }
}
