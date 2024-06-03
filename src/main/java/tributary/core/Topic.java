package tributary.core;

import java.util.List;
import java.util.ArrayList;

public class Topic<E> {
    private String id;
    private String type;
    private List<Partition> partitions = new ArrayList<Partition>();

    public Topic(String id, String type) {
        this.id = id;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public void addPartition(Partition partition) {
        partitions.add(partition);
    }

    // event generic is the same as topic
    public void addToPartition(Event<E> event, String partitionKey) {
        Partition foundPartition = partitions.stream().filter(t -> partitionKey.equals(t.getId())).findFirst()
                .orElse(null);
        foundPartition.addEvent(event);
    }

    public String show() {
        List<String> returnString = new ArrayList<String>();
        for (Partition partition : partitions) {
            int i = partition.getCurrentOffset() + 1;
            returnString.add("\tPartition ID: " + partition.getId() + "\n\tEvents:");
            for (Event<?> event : partition.getEvents()) {
                returnString.add("\t\tEvent " + i + " | " + event.toString());
                i += 1;
            }
        }
        return String.join("\n", returnString);
    }
}
