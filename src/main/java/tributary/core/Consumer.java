package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Consumer {
    private String id;
    private List<Event<?>> consumedEvents = new ArrayList<>();
    private List<Partition> allocatedPartitions = new ArrayList<>();

    public Consumer(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void addEvent(Event<?> event) {
        consumedEvents.add(event);
    }

    public void addAllocatedPartition(Partition partition) {
        allocatedPartitions.add(partition);
    }

    public void clearPartitions() {
        allocatedPartitions = new ArrayList<>();
    }

    public String showPartitions() {
        List<String> returnString = new ArrayList<String>();
        for (Partition partition : allocatedPartitions) {
            returnString.add("\t\tPartition Id: " + partition.getId());
        }
        return String.join("\n", returnString);
    }

}
