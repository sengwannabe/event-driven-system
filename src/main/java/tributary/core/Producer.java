package tributary.core;

import org.json.JSONObject;

import tributary.api.API;

public abstract class Producer<E> {
    private String id;
    private String type;
    private API cluster;

    public Producer(String id, String type, API cluster) {
        this.id = id;
        this.type = type;
        this.cluster = cluster;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public Partition getPartition(String partitionId) {
        return cluster.getPartition(partitionId);
    }

    public abstract Event<E> createEvent(Topic<?> foundTopic, String partitionId, JSONObject event);
}
