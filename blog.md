# Task 1 Preliminary Design

### Analysis Requirements

Messages
- Messages, records and events is an instantiation of a Message
- Has attributes date, id, payload, an optional partition key and value all contained within a JSON object, representing the event info.

Producers
- Random and Manual producers are an instantiation of a Producer.
- Has the responsibility to create and send events/messages to the Tributary Cluster, and optionally allocate to a specific partition within a topic, based on its allocation strategy.

Partition
- Holds a linked list/queue of messages to be consumed by the consumer
- Iterates through the linked list/queue for all events/messages to be processed.
- Has only one consumer assigned to the partition.

Topic
- Contains a list of partitions

Consumer
- Has the ability to process (consume) events/messages from one or multiple partitions
- Keeps track of consumed events/messages
- Has a method to ‘replay’ events within the partition it is consuming.

Consumer Group
- Is assigned to one topic
- Has a list of consumers that process all partitions within the specific topic
- Has the responsibility to rebalance consumers with the partitions whenever a consumer is added/removed from the group.
- Always have more or equal partitions to consumers.

TributaryCluster
- Holds a list of topics
- Is responsible for receiving events/messages from producers and assigning to the appropriate topic.

### Java API Design

Methods available for other clients to utilise, description can be extrapolated from name:
- createTopic
- createPartition
- createConsumerGroup
- createConsumer
- createProducer
- createEvent

### Usability tests
- Can create topics
- Can create partitions within an existing topic
- Can create consumer groups
- Can create consumers within an existing consumer group
- Can show different topics and their partitions alongside the partitions’ events
- Can show different consumer groups and their consumers alongside the partition allocation for the consumers
- Integer and String events can be constructed from JSON files
- Consuming an event/s will remove them from the partition’s events
- The consumer groups balancing strategy can be changed and the change will be shown to the user/tester
- Deleting a consumer from a consumer group will adjust partition allocation accordingly and show the user/tester
- Events can be produced or consumed in parallel and the appropriate messages will be shown to the user/tester in appropriate order

### Testing Plan

Basic Tests:
- Create Topic: Check that createTopic output is correct and Topic exists
- Create Partition: Check that createPartition output is correct and Partition exists
- Create ConsumerGroup: Check that createConsumerGroup output is correct and ConsumerGroupexists
- Create Consumer: Check that createConsumer output is correct
- Create Producer: Check that createProducer output is correct
- Create Event: Check that createEvent output is correct

Advanced Tests:
- Check that rebalancing methods (Range and RoundRobin) work from partitions to ConsumerGroups. This includes adding new consumers or deleting a consumer to a consumer group
- Test that consuming event works: the event should be removed from partition
- Change the ConsumerGroup balancing strategy
- Testing that random and manual producers function correctly / produce to their intended partition

More Advanced Tests (that we may not get to):
- Producers producing events in parallel
- Events can be consumed in parallel
- Events can be played back

### UML

(provided in group repository)

# Task 2

[VIDEO LINK CLICK HERE](https://drive.google.com/file/d/1xOwH-XfZKTWn6lwRsE1tqxWKNI0gMtzt/view?usp=sharing)

# Task 3 Final Design

### Final Testing Plan

Basic:
- Creating a String Topic
- Creating an Integer Topic
- Creating a String and Integer Topic
- Creating one Partition in one Topic
- Creating two Partitions in two different Topics
- Creating a ConsumerGroup
- Creating one Consumer in a ConsumerGroup
- Creating multiple Consumers across multiple ConsumerGroups
- Creating/producing a String event
- Creating/producing an Integer event
- Attempting to find a non-existent Partition
- Attempting to find a non-existent Consumer
- Showing an empty Topic
- Showing a Topic with only one Event in one Partition
- Showing a Topic with multiple Events and multiple Partitions
- Showing a ConsumerGroup with only one Consumer
- Consuming one Event
- Attempting to consume an empty partition

Advanced:
- Showing ConsumerGroup with RoundRobin balancing strategy
- Showing ConsumerGroup with Range balancing strategy
- Deleting Consumer from ConsumerGroup with RoundRobin balancing strategy
- Deleting Consumer from ConsumerGroup with Range balancing strategy
- Consuming multiple Events
- Updating ConsumerGroups rebalancing strategy from RoundRobin to Range (and vice versa)
- Replaying one event
- Replaying event starting from middle
- Attempting to replay event with larger given offset than current offset


### Final Usability Tests

(Changes from initial plan: condensing the usability tests as it would be repetitive otherwise and the tests should reflect the overall tributary working together)
- Creating String and Integer Topic with Partitions and Events inside
- Showing difference between Manual and Random producer
- Showing consuming one Event
- Showing consuming multiple Events
- Showing RoundRobin rebalance strategy when creating Consumers and deleting a Consumer
- Showing Range rebalance strategy creating Consumers and when deleting a Consumer
- Showing that events can be replayed
- Show that events being replayed are dynamic (e.g. as events are consumed, playback amount increases)

### Design Patterns

Strategy Pattern:
- The strategy pattern was used in ConsumerGroups for the Range and RoundRobin rebalancing strategies.
Abstract Factory
- Topic utilised the Abstract Factory pattern to create either the String or Integer Topic.
- Producer utilised Abstract Factory pattern to create either the String or Integer Producer. It was also required for making either the Random or Manual Producer.

### How We Accommodated for the Design Considerations

Generics
- We made it so that the Topic, Event, ConsumerGroup and Producer classes incorporated generics so that they accept either Integer or String types
Concurrency
- Could not implement in time

### UML

(provided in group repository)

### Reflection

Difficulties:
- The assignment was difficult to understand at first but as we came up with our initial design plan, the assignment started making sense.

Development Approach:
- We did a mix of ‘feature-driven’ and ‘component-driven’ development with more focus on feature-driven. We used feature-driven development for the main bulk of the assignment and which was possible as we had a clear schedule/allocation for the work. We used component-driven development for the harder aspects of the assignment that we did not grasp when initially planning our design.

