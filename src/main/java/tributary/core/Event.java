package tributary.core;

import java.time.LocalDateTime;

public class Event<E> {
    private LocalDateTime dateTimeCreated;
    private String partitionKey;
    private Object content;

    public Event(Object content, String partitionKey) {
        this.dateTimeCreated = LocalDateTime.now();
        this.partitionKey = partitionKey;
        this.content = content;
    }

    public LocalDateTime getDateTimeCreated() {
        return dateTimeCreated;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public Object getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "Content: " + content.toString();
    }
}
