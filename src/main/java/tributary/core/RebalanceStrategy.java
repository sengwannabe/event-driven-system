package tributary.core;

public interface RebalanceStrategy {
    public void execute(ConsumerGroup<?> group);
}
