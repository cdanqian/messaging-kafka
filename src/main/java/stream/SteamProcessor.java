package stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public final class SteamProcessor {

    static HashMap<String,Long> wordMap = new HashMap<>();

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-counter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());


        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumed<Long, String> consumed = Consumed.with(Serdes.Long(), Serdes.String());
        Produced<String, Long> produced = Produced.with(Serdes.String(), Serdes.Long());


        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> source = builder.stream("random-words",consumed);



        final KStream<String,Long> ks = source.map((key, value) -> {
                wordMap.put(value,wordMap.getOrDefault(value,0L)+1);
                System.out.println("I just processed " + key + " and " + value + " and counted " + wordMap.get(value));
                return new KeyValue<>(value,wordMap.get(value));
            });



        // need to override value serde to Long type
        // Store the running counts as a changelog stream to the output topic.
        ks.to("streams-wordcount-output", produced);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}