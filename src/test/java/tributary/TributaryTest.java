package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import tributary.api.API;

public class TributaryTest {
    @Test
    @DisplayName("Successfully creating String topic")
    public void testCreatingStringTopicSuccess() {
        API tributary = new API();
        String createdTopicId = tributary.createTopic("id1", "String");
        String expected = "Topic ID: id1\nType: String";
        assertEquals(expected, createdTopicId);

        createdTopicId = tributary.getTopic("id1").getId();
        assertEquals("id1", createdTopicId);
    }

    @Test
    @DisplayName("Successfully creating Integer topic")
    public void testCreatingIntegerTopicSuccess() {
        API tributary = new API();
        String createdTopicId = tributary.createTopic("id1", "Integer");
        String expected = "Topic ID: id1\nType: Integer";
        assertEquals(expected, createdTopicId);

        createdTopicId = tributary.getTopic("id1").getId();
        assertEquals("id1", createdTopicId);
    }

    @Test
    @DisplayName("Successfully creating Integer and String topic")
    public void testCreatingTwoTopics() {
        API tributary = new API();
        tributary.createTopic("id1", "Integer");
        tributary.createTopic("id2", "String");
        String createdTopicId1 = tributary.getTopic("id1").getId();
        String createdTopicId2 = tributary.getTopic("id2").getId();
        assertEquals("id1", createdTopicId1);
        assertEquals("id2", createdTopicId2);
    }

    @Test
    @DisplayName("Successfully a partition in a topic")
    public void testCreatingOnePartition() {
        API tributary = new API();
        tributary.createTopic("tId", "Integer");
        String expected = "Partition ID: pId\nAt Topic: tId";
        String actual = tributary.createPartition("pId", "tId");
        assertEquals(expected, actual);
        String partitionId = tributary.getPartition("pId").getId();
        assertEquals("pId", partitionId);
    }

    @Test
    @DisplayName("Successfully two partitions in two different topics")
    public void testCreatingTwoPartitions() {
        API tributary = new API();
        tributary.createTopic("tId1", "Integer");
        tributary.createTopic("tId2", "String");

        // First partition
        String expected = "Partition ID: pId1\nAt Topic: tId1";
        String actual = tributary.createPartition("pId1", "tId1");
        assertEquals(expected, actual);

        // Second partition
        expected = "Partition ID: pId2\nAt Topic: tId2";
        actual = tributary.createPartition("pId2", "tId2");
        assertEquals(expected, actual);

        String partitionId1 = tributary.getPartition("pId1").getId();
        String partitionId2 = tributary.getPartition("pId2").getId();

        assertEquals("pId1", partitionId1);
        assertEquals("pId2", partitionId2);
    }

    @Test
    @DisplayName("Successfully creating a consumer group")
    public void testCreatingConsumerGroup() {
        API tributary = new API();
        tributary.createTopic("tId", "String");

        String expected = "ConsumerGroup ID: cgId";
        String actual = tributary.createConsumerGroup("cgId", "tId", "RoundRobin");

        assertEquals(expected, actual);

        String foundConsumerGroup = tributary.getConsumerGroup("cgId").getId();
        assertEquals("cgId", foundConsumerGroup);
    }

    @Test
    @DisplayName("Successfully creating one consumer")
    public void testCreatingOneConsumer() {
        API tributary = new API();
        tributary.createTopic("tId", "String");
        tributary.createConsumerGroup("cgId", "tId", "Range");
        String expected = "Consumer ID: consumer\nIn consumerGroup: cgId";
        String actual = tributary.createConsumer("cgId", "consumer");

        assertEquals(expected, actual);

        String foundConsumer = tributary.getConsumer("consumer").getId();

        assertEquals("consumer", foundConsumer);
    }

    @Test
    @DisplayName("Successfully creating multiple consumers across multiple consumer groups")
    public void testCreatingMultipleConsumers() {
        API tributary = new API();
        tributary.createTopic("tId", "String");
        tributary.createConsumerGroup("cgId1", "tId", "Range");
        tributary.createConsumerGroup("cgId2", "tId", "Range");
        tributary.createConsumerGroup("cgId3", "tId", "RoundRobin");
        tributary.createConsumer("cgId1", "consumer1");
        tributary.createConsumer("cgId2", "consumer2");
        tributary.createConsumer("cgId3", "consumer3");
        String foundConsumer1 = tributary.getConsumer("consumer1").getId();
        String foundConsumer2 = tributary.getConsumer("consumer2").getId();
        String foundConsumer3 = tributary.getConsumer("consumer3").getId();

        assertEquals("consumer1", foundConsumer1);
        assertEquals("consumer2", foundConsumer2);
        assertEquals("consumer3", foundConsumer3);
    }

    @Test
    @DisplayName("Successfully produce/create a String event")
    public void testStringProducer() {
        API tributary = new API();
        tributary.createProducer("prodId", "String", "Random");
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId", "tId");

        // Creating JSON object for event
        JSONObject json = new JSONObject();
        json.put("content", "string one");

        String expected = "Event Content: string one\nPartition ID: partId";
        String actual = tributary.createEvent("prodId", "tId", "partId", json);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Successfully produce/create an Integer event")
    public void testIntegerProducer() {
        API tributary = new API();
        tributary.createProducer("prodId", "Integer", "Manual");
        tributary.createTopic("tId", "Integer");
        tributary.createPartition("partId", "tId");

        // Creating JSON object for event
        JSONObject json = new JSONObject();
        json.put("content", 22);

        String expected = "Event Content: 22\nPartition ID: partId";
        String actual = tributary.createEvent("prodId", "tId", "partId", json);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Trying to find a non-existent partition")
    public void testNonExistentPartition() {
        API tributary = new API();

        // Setting up topic/partition
        tributary.createTopic("tId", "Integer");
        tributary.createPartition("realpartition", "tId");

        assertEquals(null, tributary.getPartition("Realpartition"));
    }

    @Test
    @DisplayName("Trying to find a non-existent consumer")
    public void testNonExistentConsumer() {
        API tributary = new API();

        // Setting up topic/partition
        tributary.createTopic("topic", "String");
        tributary.createConsumerGroup("group", "topic", "RoundRobin");
        tributary.createConsumer("group", "consumer");

        assertEquals(null, tributary.getConsumer("consumer?"));
    }

    @Test
    @DisplayName("Show empty topic")
    public void testShowEmptyTopic() {
        API tributary = new API();

        // Setting up producer/topic/partition
        tributary.createTopic("tId", "Integer");

        // Creating expected output string
        String expected = "Topic ID: tId\nPartitions: \n";

        String actual = tributary.showTopic("tId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show topic with one event and one partition")
    public void testShowTopicOneEventPartition() {
        API tributary = new API();

        // Setting up producer/topic/partition
        tributary.createProducer("prodId", "Integer", "Random");
        tributary.createTopic("tId", "Integer");
        tributary.createPartition("partId", "tId");

        // Creating JSON object for event
        JSONObject json = new JSONObject();
        json.put("content", 100);

        tributary.createEvent("prodId", "tId", "partId", json);

        // Creating expected output string
        String expected = "Topic ID: tId\nPartitions: \n";
        expected += "\tPartition ID: partId\n\tEvents:\n";
        expected += "\t\tEvent 1 | Content: 100";

        String actual = tributary.showTopic("tId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show topic with multiple events and partition")
    public void testShowTopicMultiplePartitionsEvents() {
        API tributary = new API();

        // Setting up producer/topic/partitions
        tributary.createProducer("prodId", "String", "Manual");

        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");

        // Creating JSON objects for events
        JSONObject json1 = new JSONObject();
        json1.put("content", "hey");
        JSONObject json2 = new JSONObject();
        json2.put("content", "hello");
        JSONObject json3 = new JSONObject();
        json3.put("content", "howdy");

        tributary.createEvent("prodId", "tId", "partId1", json1);
        tributary.createEvent("prodId", "tId", "partId2", json2);
        tributary.createEvent("prodId", "tId", "partId1", json3);

        // Creating expected output string
        String expected = "Topic ID: tId\nPartitions: \n";
        expected += "\tPartition ID: partId1\n\tEvents:\n";
        expected += "\t\tEvent 1 | Content: hey\n";
        expected += "\t\tEvent 2 | Content: howdy\n";
        expected += "\tPartition ID: partId2\n\tEvents:\n";
        expected += "\t\tEvent 1 | Content: hello";

        String actual = tributary.showTopic("tId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show consumer group with one consumer")
    public void testShowConsumerGroupOneConsumer() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumer
        tributary.createTopic("tId", "String");
        tributary.createConsumerGroup("cgId", "tId", "RoundRobin");
        tributary.createConsumer("cgId", "consumer");

        // Creating expected output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer\n\tAllocated partitions:\n";

        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show consumer group with round robin balancing strategy")
    public void testShowConsumerGroupRoundRobin() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumers/partitions
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");
        tributary.createPartition("partId3", "tId");
        tributary.createPartition("partId4", "tId");

        tributary.createConsumerGroup("cgId", "tId", "RoundRobin");
        tributary.createConsumer("cgId", "consumer1");
        tributary.createConsumer("cgId", "consumer2");

        // Creating expected output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId3\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\t\tPartition Id: partId4";

        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show consumer group with range balancing strategy")
    public void testShowConsumerGroupRange() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumers/partitions
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");
        tributary.createPartition("partId3", "tId");
        tributary.createPartition("partId4", "tId");

        tributary.createConsumerGroup("cgId", "tId", "Range");
        tributary.createConsumer("cgId", "consumer1");
        tributary.createConsumer("cgId", "consumer2");

        // Creating expected output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId3\n";
        expected += "\t\tPartition Id: partId4";

        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show round robin consumer group with rebalanced with deleted consumer")
    public void testDeleteRoundRobinGroup() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumers/partitions
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");
        tributary.createPartition("partId3", "tId");
        tributary.createPartition("partId4", "tId");

        tributary.createConsumerGroup("cgId", "tId", "RoundRobin");
        tributary.createConsumer("cgId", "consumer1");
        tributary.createConsumer("cgId", "consumer2");
        tributary.createConsumer("cgId", "consumer3");

        // Creating initial output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId4\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\tConsumer ID: consumer3\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId3";

        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);

        // Creating deleted output string
        expected = "Deleted consumer ID: consumer1\nRebalanced consumer group:\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId3\n";
        expected += "\tConsumer ID: consumer3\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\t\tPartition Id: partId4";

        actual = tributary.deleteConsumer("consumer1");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show range consumer group with rebalanced with deleted consumer")
    public void testDeleteRangeGroup() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumers/partitions
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");
        tributary.createPartition("partId3", "tId");
        tributary.createPartition("partId4", "tId");

        tributary.createConsumerGroup("cgId", "tId", "Range");
        tributary.createConsumer("cgId", "consumer1");
        tributary.createConsumer("cgId", "consumer2");
        tributary.createConsumer("cgId", "consumer3");

        // Creating initial output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId3\n";
        expected += "\tConsumer ID: consumer3\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId4";

        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);

        // Creating deleted output string
        expected = "Deleted consumer ID: consumer1\nRebalanced consumer group:\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\tConsumer ID: consumer3\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId3\n";
        expected += "\t\tPartition Id: partId4";

        actual = tributary.deleteConsumer("consumer1");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Range consumer group with more consumers than partitions")
    public void testMorePartitionsThanConsumersRange() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumers/partitions
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");

        tributary.createConsumerGroup("cgId", "tId", "Range");
        tributary.createConsumer("cgId", "consumer1");
        tributary.createConsumer("cgId", "consumer2");
        tributary.createConsumer("cgId", "consumer3");

        // Creating initial output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\tConsumer ID: consumer3\n\tAllocated partitions:\n";

        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Consume one event")
    public void testConsumeOneEvent() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumer/partition/event/producer
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId", "tId");
        tributary.createConsumerGroup("cgId", "tId", "Range");
        tributary.createConsumer("cgId", "consumer");
        tributary.createProducer("prodId", "String", "Random");
        JSONObject json = new JSONObject();
        json.put("content", "matcha latte");
        tributary.createEvent("prodId", "tId", null, json);

        // Creating expected output string
        String expected = "Event content: matcha latte\nConsumer ";
        expected += "'consumer' received event\n";

        // Consume event
        String actual = tributary.consumeEvent("consumer", "partId");
        assertEquals(expected, actual);

        // Check that consumed event is removed from partition
        expected = "Topic ID: tId\nPartitions: \n";
        expected += "\tPartition ID: partId\n\tEvents:";

        actual = tributary.showTopic("tId");

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Consume multiple events")
    public void testConsumeMultipleEvents() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumer/partition/events/producer
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId", "tId");
        tributary.createConsumerGroup("cgId", "tId", "RoundRobin");
        tributary.createConsumer("cgId", "consumer");
        tributary.createProducer("prodId", "String", "Random");
        JSONObject json1 = new JSONObject();
        JSONObject json2 = new JSONObject();
        JSONObject json3 = new JSONObject();
        JSONObject json4 = new JSONObject();
        json1.put("content", "apple");
        json2.put("content", "banana");
        json3.put("content", "coconut");
        json4.put("content", "dragon fruit");

        tributary.createEvent("prodId", "tId", "partId", json1);
        tributary.createEvent("prodId", "tId", "partId", json2);
        tributary.createEvent("prodId", "tId", "partId", json3);
        tributary.createEvent("prodId", "tId", "partId", json4);

        // Creating expected output string
        String expected = "Event content: apple\nConsumer ";
        expected += "'consumer' received event\n\n";
        expected += "Event content: banana\nConsumer ";
        expected += "'consumer' received event\n\n";
        expected += "Event content: coconut\nConsumer ";
        expected += "'consumer' received event\n";

        // Consume event
        String actual = tributary.consumeEvents(3, "consumer", "partId");
        assertEquals(expected, actual);

        // Check that consumed event is removed from partition
        expected = "Topic ID: tId\nPartitions: \n";
        expected += "\tPartition ID: partId\n\tEvents:\n";
        expected += "\t\tEvent 4 | Content: dragon fruit";

        actual = tributary.showTopic("tId");

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Update consumer group's rebalance strategy from RoundRobin to Range")
    public void testRoundRobinToRange() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumers/partitions
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");
        tributary.createPartition("partId3", "tId");

        tributary.createConsumerGroup("cgId", "tId", "RoundRobin");
        tributary.createConsumer("cgId", "consumer1");
        tributary.createConsumer("cgId", "consumer2");

        // Creating initial RoundRobin output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId3\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId2";
        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);

        // Switch balancing strategy
        expected = "ConsumerGroup 'cgId' changed rebalancing allocation to Range";
        actual = tributary.setRebalance("cgId", "Range");
        assertEquals(expected, actual);

        // Creating new Range output string
        expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId3";

        actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Update consumer group's rebalance strategy from Range to Roundrobin")
    public void testRangeToRoundRobin() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumers/partitions
        tributary.createTopic("tId", "String");
        tributary.createPartition("partId1", "tId");
        tributary.createPartition("partId2", "tId");
        tributary.createPartition("partId3", "tId");

        tributary.createConsumerGroup("cgId", "tId", "Range");
        tributary.createConsumer("cgId", "consumer1");
        tributary.createConsumer("cgId", "consumer2");

        // Creating initial Range output string
        String expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId2\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId3";
        String actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);

        // Switch balancing strategy
        expected = "ConsumerGroup 'cgId' changed rebalancing allocation to RoundRobin";
        actual = tributary.setRebalance("cgId", "RoundRobin");
        assertEquals(expected, actual);

        // Creating new Range output string
        expected = "ConsumerGroup ID: cgId\nConsumers:\n";
        expected += "\tConsumer ID: consumer1\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId1\n";
        expected += "\t\tPartition Id: partId3\n";
        expected += "\tConsumer ID: consumer2\n\tAllocated partitions:\n";
        expected += "\t\tPartition Id: partId2";

        actual = tributary.showGroup("cgId");
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Trying to consume an empty partition")
    public void testConsumingEmptyPartition() {
        API tributary = new API();

        // Setting up topic/partition
        tributary.createTopic("topic", "String");
        tributary.createPartition("partition", "topic");
        tributary.createConsumerGroup("group", "topic", "RoundRobin");
        tributary.createConsumer("group", "consumer");

        assertEquals("No event to consume", tributary.consumeEvent("consumer", "partition"));
    }

    @Test
    @DisplayName("Replaying one event")
    public void testReplayOneEvent() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumer/partition/events/producer
        tributary.createTopic("topic", "String");
        tributary.createPartition("partId", "topic");
        tributary.createConsumerGroup("cgId", "topic", "RoundRobin");
        tributary.createConsumer("cgId", "consumer");
        tributary.createProducer("prodId", "String", "Random");
        JSONObject json1 = new JSONObject();
        json1.put("content", "wait trait rate date");

        tributary.createEvent("prodId", "topic", "partId", json1);

        tributary.consumeEvents(1, "consumer", "partId");

        String expectedString = "Playing back events:\n\n";
        expectedString += "Event 1 | Content: wait trait rate date\nConsumer 'consumer' received event\n";

        String actual = tributary.playbackEvents("consumer", "partId", 1);

        assertEquals(expectedString, actual);
    }

    @Test
    @DisplayName("Replaying messages from the middle")
    public void testReplayMessagesInMiddle() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumer/partition/events/producer
        tributary.createTopic("topic", "String");
        tributary.createPartition("partId", "topic");
        tributary.createConsumerGroup("cgId", "topic", "RoundRobin");
        tributary.createConsumer("cgId", "consumer");
        tributary.createProducer("prodId", "String", "Random");
        JSONObject json1 = new JSONObject();
        JSONObject json2 = new JSONObject();
        JSONObject json3 = new JSONObject();
        JSONObject json4 = new JSONObject();
        json1.put("content", "one");
        json2.put("content", "due");
        json3.put("content", "trois");
        json4.put("content", "bon");

        tributary.createEvent("prodId", "topic", "partId", json1);
        tributary.createEvent("prodId", "topic", "partId", json2);
        tributary.createEvent("prodId", "topic", "partId", json3);
        tributary.createEvent("prodId", "topic", "partId", json4);

        tributary.consumeEvents(3, "consumer", "partId");

        // Note that since three events were consumed, current offset is 3.
        // This means that it should only replay messages from offset 2 to 3 or 'due' to 'trois'
        String expectedString = "Playing back events:\n\n";
        expectedString += "Event 2 | Content: due\nConsumer 'consumer' received event\n";
        expectedString += "Event 3 | Content: trois\nConsumer 'consumer' received event\n";

        String actual = tributary.playbackEvents("consumer", "partId", 2);

        assertEquals(expectedString, actual);
    }

    @Test
    @DisplayName("Attempting to playback where given offset is larger than current offset")
    public void testPlaybackOffsetExceedsCurrentOffset() {
        API tributary = new API();

        // Setting up topic/consumergroup/consumer/partition/events/producer
        tributary.createTopic("topic", "String");
        tributary.createPartition("partId", "topic");
        tributary.createConsumerGroup("cgId", "topic", "RoundRobin");
        tributary.createConsumer("cgId", "consumer");
        tributary.createProducer("prodId", "String", "Random");
        JSONObject json1 = new JSONObject();
        json1.put("content", "yo");

        tributary.createEvent("prodId", "topic", "partId", json1);

        tributary.consumeEvents(1, "consumer", "partId");

        String expectedString = "Offset provided larger than current offset in given partition";

        String actual = tributary.playbackEvents("consumer", "partId", 2);

        assertEquals(expectedString, actual);
    }
}
