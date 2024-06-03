package tributary.core;

import java.util.stream.Stream;

public class RangeRebalance implements RebalanceStrategy {
    @Override
    public void execute(ConsumerGroup<?> group) {
        Partition[] partitions = group.getTopicPartitions().toArray(new Partition[0]);
        Consumer[] consumers = group.getConsumers().toArray(new Consumer[0]);

        if (consumers.length == 0 || partitions.length == 0) {
            return;
        }

        int currentCountP = 0;
        Stream.of(consumers).forEach(c -> c.clearPartitions());

        int eventAllocation = partitions.length / consumers.length;
        int remainingAllocation = partitions.length % consumers.length;

        // allocate partitions evenly
        for (Consumer consumer : consumers) {
            int endPCount = currentCountP + eventAllocation;
            if (remainingAllocation > 0) {
                // if-statement is for allocating remainders
                endPCount += 1;
                remainingAllocation -= 1;
            }
            while (currentCountP < endPCount) {
                consumer.addAllocatedPartition(partitions[currentCountP]);
                currentCountP++;
            }
        }
    }
}
