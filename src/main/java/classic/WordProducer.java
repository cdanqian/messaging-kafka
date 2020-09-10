package classic;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class WordProducer {

    private final static String TOPIC = "random-words";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static KafkaProducer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "word-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyWordPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }
    static void runProducer(final int sendMessageCount) throws Exception {
        final KafkaProducer<Long, String> producer = createProducer();
        long startTime = System.currentTimeMillis();

        try {
            while(true) {
                long time = System.currentTimeMillis() - startTime;

                String words[] = {"hello", "world", "friend", "mine", "when","how","difficult"};

                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC,  time/1000L, words[(int)(Math.random()*words.length)]);

                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d)\n",
                        record.key(), record.value(), metadata.partition());
                Thread.sleep(1000);

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducer(5);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }


}
