import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;

public class StreamsProcessor implements Processor<String, String, String, String> {

    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext<String, String> processorContext) {

        this.store = processorContext.getStateStore("test-store");
        processorContext.schedule(Duration.ofSeconds(3), WALL_CLOCK_TIME, this::punctuate);
    }

    public void punctuate(long timestamp) {

        System.out.println("punctuate " + Instant.now());

        try (KeyValueIterator<String, String> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, String> keyValue = iterator.next();

                System.out.println(keyValue.key + keyValue.value);
            }
        }
    }

    @Override
    public void process(Record<String, String> record) {
        System.out.println(record.key() + " " + record.value());

        store.put(record.key(), record.value());
    }

    @Override
    public void close() {}
}