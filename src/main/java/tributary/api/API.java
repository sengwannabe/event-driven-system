package tributary.api;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import tributary.core.Topic;
import tributary.core.TopicFactory;
import tributary.core.ConsumerGroup;
import tributary.core.Consumer;
import tributary.core.ProducerFactory;
import tributary.core.Producer;
import tributary.core.Partition;
import tributary.core.Event;

public class API {
    private TopicFactory topicFactory = new TopicFactory();
    private List<Topic<?>> topics = new ArrayList<Topic<?>>();
    private List<ConsumerGroup<?>> consumerGroups = new ArrayList<ConsumerGroup<?>>();
    private ProducerFactory producerFactory = new ProducerFactory();
    private List<Producer<?>> producers = new ArrayList<Producer<?>>();

    /**
     *
     * @param id id of the producer
     * @return either the id of an existing producer or null
     * @pre N/A
     * @post N/A
     */
    public Producer<?> getProducer(String id) {
        return producers.stream().filter(p -> id.equals(p.getId())).findFirst().orElse(null);
    }

    /**
     *
     * @param id id of the partition
     * @return either the id of an existing partition or null
     * @pre N/A
     * @post N/A
     */
    public Partition getPartition(String id) {
        for (Topic<?> topic : topics) {
            Partition found = topic.getPartitions().stream().filter(p -> id.equals(p.getId())).findFirst().orElse(null);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     *
     * @param id id of topic
     * @return either the id of an existing topic or null
     * @pre N/A
     * @post N/A
     */
    public Topic<?> getTopic(String id) {
        return topics.stream().filter(t -> id.equals(t.getId())).findFirst().orElse(null);
    }

    /**
     *
     * @param id id of consumer group
     * @return either the id of an consumer group or null
     * @pre N/A
     * @post N/A
     */
    public ConsumerGroup<?> getConsumerGroup(String id) {
        return consumerGroups.stream().filter(g -> id.equals(g.getId())).findFirst().orElse(null);
    }

    /**
     *
     * @param id id of consumer
     * @return either the id of an existing consumer or null
     * @pre N/A
     * @post N/A
     */
    public Consumer getConsumer(String id) {
        for (ConsumerGroup<?> group : consumerGroups) {
            Consumer consumer = group.getConsumer(id);
            if (consumer != null) {
                return consumer;
            }
        }
        return null;
    }

    /**
     *
     * @param topicId id of new topic
     * @param eventType type of event related to Topic
     * @return string confirming creation
     * @pre type is either "Integer" or "String", id isn't already used by another topic
     * @post Topic added to topics list
     */
    public String createTopic(String topicId, String eventType) {
        Topic<?> topic = topicFactory.makeTopic(topicId, eventType);
        topics.add(topic);
        return "Topic ID: " + topicId + "\nType: " + eventType;
    }

    /**
     *
     * @param partitionId id of new partition
     * @param topicId id of an existing topic
     * @return string confirming creation
     * @pre partitionId isn't already used by another partition, topicId exists
     * @post Partition added to the given topic's partition list
     */
    public String createPartition(String partitionId, String topicId) {
        Partition partition = new Partition(partitionId);
        Topic<?> foundTopic = getTopic(topicId);
        foundTopic.addPartition(partition);
        return "Partition ID: " + partitionId + "\nAt Topic: " + topicId;
    }

    /**
     *
     * @param groupId id of the new consumer group
     * @param topicId id of an existing topic
     * @param rebalancing balancing strategy of either "Range" or "RoundRobin"
     * @return string confirming creation
     * @pre rebalancing is either "Range" or "RoundRobin", topicId exists in cluster,
     * groupId isn't already used by existing group
     * @post new ConsumerGroup added to consumerGroups list
     */
    public String createConsumerGroup(String groupId, String topicId, String rebalancing) {
        Topic<?> topic = getTopic(topicId);
        ConsumerGroup<?> consumerGroup = new ConsumerGroup<>(groupId, topic, rebalancing);
        consumerGroups.add(consumerGroup);
        return "ConsumerGroup ID: " + groupId;
    }

    /**
     *
     * @param groupId id of an existing consumer group
     * @param consumerId id of the new consumer
     * @return string confirming creation
     * @pre groupId exists, consumerId isn't already taken by another consumer
     * @post Consumer added to the given consumerGroup, consumer group will be rebalanced accordingly
     */
    public String createConsumer(String groupId, String consumerId) {
        Consumer consumer = new Consumer(consumerId);
        ConsumerGroup<?> foundConsumerGroup = consumerGroups.stream().filter(g -> groupId.equals(g.getId())).findFirst()
                .orElse(null);
        foundConsumerGroup.addConsumer(consumer);
        return "Consumer ID: " + consumerId + "\nIn consumerGroup: " + groupId;
    }

    /**
     *
     * @param producerId id of the new producer
     * @param eventType type of the events to be produced by the producer
     * @param allocation type of allocation for consumer group of either "Random" or "Manual"
     * @return string confirming creation
     * @pre producerId isn't already taken by another producer, type is either "String" or "Integer"
     * allocation is either "Random" or "Manual"
     * @post Producer added to producers list in cluster
     */
    public String createProducer(String producerId, String eventType, String allocation) {
        switch (allocation) {
        case "Random":
            producers.add(producerFactory.makeRandomProducer(producerId, this, eventType));
            break;
        case "Manual":
            producers.add(producerFactory.makeManualProducer(producerId, this, eventType));
            break;
        default:
            break;
        }
        return "Producer ID: " + producerId + "\nProduces type: " + eventType;
    }

    /**
     * Creates an event and adds it to the partition and producer
     * @param producerId id of an existing producer
     * @param topicId id of an existing topic
     * @param partitionKey id of an existing partition (if producer is 'Manual')
     * @return string confirming creation
     * @param event JSON object that contains the event
     * @pre JSONObject contains field called "content", all given IDs exist in the cluster
     * @post event added to partition
     */
    public String createEvent(String producerId, String topicId, String partitionKey, JSONObject event) {
        // creates message from producer, and adds to corresponding partition

        Producer<?> foundProducer = getProducer(producerId);
        Topic<?> foundTopic = getTopic(topicId);
        Event<?> createdEvent = foundProducer.createEvent(foundTopic, partitionKey, event);

        return "Event Content: " + createdEvent.getContent() + "\nPartition ID: " + createdEvent.getPartitionKey();
    }

    /**
     *
     * @param consumerId id of an existing consumer
     * @return string confirming deletion
     * @pre consumerId exists
     * @post the consumer will be removed from its consumerGroup, the consumerGroup will be rebalanced accordingly
     */
    public String deleteConsumer(String consumerId) {
        for (ConsumerGroup<?> group : consumerGroups) {
            Consumer consumer = group.getConsumer(consumerId);
            if (consumer != null) {
                group.removeConsumer(consumer);
                return "Deleted consumer ID: " + consumerId + "\nRebalanced consumer group:\n" + group.show();
            }
        }
        return "";
    }

    /**
     *
     * @param consumerId id of consumer
     * @param partitionId id of partition to consume from
     * @return string confirming consumption
     * @pre consumerId and partitionId both exists
     * @post the consumer will consume an event if there is a event in the partition
     */
    public String consumeEvent(String consumerId, String partitionId) {
        Consumer consumer = getConsumer(consumerId);
        Partition partition = getPartition(partitionId);
        Event<?> event = partition.getNextEvent();
        if (event == null) {
            return "No event to consume";
        }
        consumer.addEvent(event);
        return "Event content: " + event.getContent() + "\nConsumer '" + consumerId + "' received event\n";
    }

    /**
     *
     * @param numberEvents number of events consumer will consume
     * @param consumerId id of consumer
     * @param partitionId id of partition to consume from
     * @return string confirming consumptions
     * @pre consumerId and partitionId exists, number of events is greater than 0,
     * numberEvents <= number of events in partition
     * @post the consumer will attain the number of events specified in its consumption
     */
    public String consumeEvents(int numberEvents, String consumerId, String partitionId) {
        List<String> returnStrings = new ArrayList<String>();
        for (int i = 0; i < numberEvents; i++) {
            returnStrings.add(consumeEvent(consumerId, partitionId));
        }
        return String.join("\n", returnStrings);
    }

    /**
     *
     * @param topicId id of an existing topic
     * @return string containing relevant information
     * @pre topicId exists
     * @post N/A
     */
    public String showTopic(String topicId) {
        Topic<?> topic = getTopic(topicId);
        return "Topic ID: " + topic.getId() + "\nPartitions: \n" + topic.show();
    }

    /**
     *
     * @param groupId id of an existing consumer group
     * @return string containing relevant information
     * @pre groupId exists
     * @post N/A
     */
    public String showGroup(String groupId) {
        ConsumerGroup<?> group = getConsumerGroup(groupId);
        return "ConsumerGroup ID: " + group.getId() + "\nConsumers:\n" + group.show();
    }

    /**
     *
     * @param groupId id of an existing consumer group
     * @param rebalancing string of either "RoundRobin" or "Range"
     * @return string containing comfirmation of switching
     * @pre groupId exists in cluster, rebalancing string is either "RoundRobin" or "Range"
     * @post consumer group's new rebalancing strategy will update partition allocation accordingly
     */
    public String setRebalance(String groupId, String rebalancing) {
        ConsumerGroup<?> group = getConsumerGroup(groupId);
        group.setRebalance(rebalancing);
        return "ConsumerGroup '" + group.getId() + "' changed rebalancing allocation to " + rebalancing;
    }

    /**
     *
     * @param type string of either "Producer" or "Topic"
     * @param id id of the type to be searched for
     * @return type of the searched for id
     * @pre type is either "Producer" or "Topic", the id will exist in the given type
     * @post N/A
     */
    public String getType(String type, String id) {
        String res = "";
        if (type.equals("Topic")) {
            res = getTopic(id).getType();
        } else if (type.equals("Producer")) {
            res = getProducer(id).getType();
        }

        return res;
    }

    /**
     *
     * @param consumerId id of an existing consumer
     * @param partitionId id of existing partition
     * @param offset the desired offset
     * @return String showing events replayed
     * @pre consumerId and partitionId exists, offset > 0
     * @post N/A
     */
    public String playbackEvents(String consumerId, String partitionId, int offset) {
        Consumer consumer = getConsumer(consumerId);
        Partition partition = getPartition(partitionId);
        // offset - 1 is to account for java zero indexing arrays / user inputting offset indexed from 1
        return partition.replayEvent(offset - 1, consumer);
    }
}
