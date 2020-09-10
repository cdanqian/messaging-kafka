package classic;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class MyWordPartitioner implements Partitioner {

    private static final int PARTITION_COUNT=8;


    public MyWordPartitioner() {

    }

    @Override

    public void configure(java.util.Map<java.lang.String,?> configs){

    }

    @Override

    public int partition(java.lang.String topic, java.lang.Object key, byte[] keyBytes, java.lang.Object value, byte[] valueBytes, Cluster cluster) {
        Long keyValue = Long.parseLong(key.toString());
        return (int)(keyValue % PARTITION_COUNT);

    }
    @Override

    public void close() {

    }
}