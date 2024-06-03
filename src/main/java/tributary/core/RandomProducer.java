package tributary.core;

import java.util.List;
import java.util.Random;

import org.json.JSONObject;

import tributary.api.API;

public class RandomProducer<E> extends Producer<E> {
    public RandomProducer(String id, String type, API cluster) {
        super(id, type, cluster);
    }

    /**
     * @precondition partitionKey is null since random producer randomly selects partitionKey
     */
    @Override
    public Event<E> createEvent(Topic<?> foundTopic, String partitionKey, JSONObject event) {
        List<Partition> topicPartitions = foundTopic.getPartitions();
        Random random = new Random();
        int randomIndex = random.nextInt(topicPartitions.size());
        Partition partition = topicPartitions.get(randomIndex);

        Object content = event.get("content");
        Event<E> createdEvent = new Event<E>(content, partition.getId());
        partition.addEvent(createdEvent);

        return createdEvent;
    }
}
