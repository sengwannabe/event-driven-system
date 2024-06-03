package tributary.core;

import java.util.stream.Stream;

public class RoundRobinRebalance implements RebalanceStrategy {
    @Override
    public void execute(ConsumerGroup<?> group) {
        Partition[] partitions = group.getTopicPartitions().toArray(new Partition[0]);
        Consumer[] consumers = group.getConsumers().toArray(new Consumer[0]);

        if (consumers.length == 0 || partitions.length == 0) {
            return;
        }

        int consumerCount = consumers.length;
        int currentCountC = 0;

        Stream.of(consumers).forEach(c -> c.clearPartitions());

        // allocate partitions
        for (Partition partition : partitions) {
            if (currentCountC == consumerCount) {
                currentCountC = 0;
            }
            consumers[currentCountC].addAllocatedPartition(partition);
            currentCountC++;
        }

    }
}
