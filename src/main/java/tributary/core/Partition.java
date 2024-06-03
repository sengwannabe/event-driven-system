package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Partition {
    private String id;
    private List<Event<?>> events = new ArrayList<Event<?>>();
    private int currentOffset = 0;

    public Partition(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    // message generic can be any since dont know generic of partition
    public void addEvent(Event<?> event) {
        events.add(event);
    }

    public List<Event<?>> getEvents() {
        return events.subList(currentOffset, events.size());
    }

    public Event<?> getNextEvent() {
        if (currentOffset >= events.size()) {
            return null;
        }
        Event<?> event = events.get(currentOffset);
        currentOffset++;
        return event;
    }

    public String replayEvent(int offset, Consumer consumer) {
        if (offset >= currentOffset) {
            return "Offset provided larger than current offset in given partition";
        }
        String res = "Playing back events:\n\n";
        while (offset < currentOffset) {
            int displayOffset = offset + 1;
            res += "Event " + displayOffset + " | " + events.get(offset).toString() + "\n";
            res += "Consumer '" + consumer.getId() + "' received event\n";
            offset++;
        }
        return res;
    }

    public int getCurrentOffset() {
        return currentOffset;
    }

}
