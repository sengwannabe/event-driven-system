package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup<T> {
    // Will probs require observer pattern for rebalancing strategy
    private List<Consumer> consumers = new ArrayList<Consumer>();
    private String id;
    private Topic<T> topic;
    private RebalanceStrategy rebalanceStrategy;

    public ConsumerGroup(String id, Topic<T> topic, String rebalance) {
        this.id = id;
        this.topic = topic;
        if (rebalance.equals("Range")) {
            this.rebalanceStrategy = new RangeRebalance();
        } else if (rebalance.equals("RoundRobin")) {
            this.rebalanceStrategy = new RoundRobinRebalance();
        }
    }

    public String getId() {
        return id;
    }

    public List<Consumer> getConsumers() {
        return consumers;
    }

    public List<Partition> getTopicPartitions() {
        return topic.getPartitions();
    }

    public void addConsumer(Consumer consumer) {
        consumers.add(consumer);
        rebalanceStrategy.execute(this);
    }

    public Consumer getConsumer(String consumerId) {
        return consumers.stream().filter(c -> consumerId.equals(c.getId())).findFirst().orElse(null);
    }

    public void removeConsumer(Consumer consumer) {
        consumers.remove(consumer);
        rebalanceStrategy.execute(this);
    }

    public void setRebalance(String rebalance) {
        if (rebalance.equals("Range")) {
            rebalanceStrategy = new RangeRebalance();
        } else if (rebalance.equals("RoundRobin")) {
            rebalanceStrategy = new RoundRobinRebalance();
        }
        rebalanceStrategy.execute(this);
    }

    public String show() {
        List<String> returnString = new ArrayList<String>();
        for (Consumer consumer : consumers) {
            returnString.add(
                    "\tConsumer ID: " + consumer.getId() + "\n\tAllocated partitions:\n" + consumer.showPartitions());
        }
        return String.join("\n", returnString);
    }
}
