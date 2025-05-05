import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.List;

public class CreateTopics {
    public static void main(String[] args) {
        try (AdminClient client = AdminClientFactory.adminClient()) {

            String topic = args[0];
            int partitions = Integer.parseInt(args[1]);
            short replication = 1;

            List<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic(topic, partitions, replication));
            client.createTopics(topics);
        }
    }
}
