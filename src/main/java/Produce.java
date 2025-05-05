import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Produce {
    public static void main(String[] args) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        //props.put(ProducerConfig.ACKS_CONFIG, "all");
        //props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        String topic = args[0];
        String key = args[1];
        String value = args[2];

        try (KafkaProducer<String,String> producer = new KafkaProducer<>(props)) {
            var record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
        }
    }
}
