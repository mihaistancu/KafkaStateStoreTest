import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

public class Streams {
    public static void main(String[] args) throws Exception {

        String[] topics = args[0].split(",");
        String app = args[1];

        StoreBuilder<KeyValueStore<String, String>> schedulerStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("test-store"),
                Serdes.String(),
                Serdes.String());

        var builder = new StreamsBuilder();

        builder.addStateStore(schedulerStoreBuilder);

        for (String topic: topics) {
            builder
                    .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                    .process(StreamsProcessor::new, "test-store");
        }

        var topology = builder.build();

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, app);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 900000);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/" + app + "-" + UUID.randomUUID());

        var latch = new CountDownLatch(1);

        try (var streams = new KafkaStreams(topology, properties)) {

            streams.setUncaughtExceptionHandler(exception -> REPLACE_THREAD);

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.close(Duration.ofSeconds(5));
                    latch.countDown();
                }
            });

            streams.start();
            latch.await();
        }
    }
}
