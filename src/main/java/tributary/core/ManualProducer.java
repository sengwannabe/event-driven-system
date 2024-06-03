package tributary.core;

import org.json.JSONObject;

import tributary.api.API;

public class ManualProducer<E> extends Producer<E> {
    public ManualProducer(String id, String type, API cluster) {
        super(id, type, cluster);
    }

    @Override
    public Event<E> createEvent(Topic<?> foundTopic, String partitionKey, JSONObject event) {
        Object content = event.get("content");
        Event<E> createdEvent = new Event<E>(content, partitionKey);
        Partition partition = getPartition(partitionKey);
        partition.addEvent(createdEvent);
        return createdEvent;
    }
}
