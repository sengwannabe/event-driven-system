package tributary.cli;

import java.util.Arrays;
import java.util.Scanner;

import org.json.JSONObject;

import tributary.api.API;

public class TributaryCLI {
    private API cluster = new API();

    private void createConsumerOrGroup(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Not enough arguments");
        }
        switch (args[0]) {
        case "group":
            // command is to create consumer group
            if (args.length > 4) {
                throw new IllegalArgumentException("Too many arguments");
            }

            String groupId = args[1];

            String topicId = args[2];
            if (cluster.getTopic(topicId) == null) {
                throw new IllegalArgumentException("TopicId not found");
            }

            String rebalancing = args[3];
            System.out.println(cluster.createConsumerGroup(groupId, topicId, rebalancing));
            break;
        default:
            // command is to create a consumer
            if (args.length > 2) {
                throw new IllegalArgumentException("Too many arguments");
            }

            String groupId1 = args[0];
            if (cluster.getConsumerGroup(groupId1) == null) {
                throw new IllegalArgumentException("ConsumerGroupId not found");
            }

            String consumerId = args[1];
            System.out.println(cluster.createConsumer(groupId1, consumerId));
            break;
        }
    }

    private void create(String[] args) throws Exception {
        switch (args[0]) {
        case "topic":
            if (args.length > 3) {
                throw new IllegalArgumentException("Too many arguments");
            }

            String topicId = args[1];
            String eventType = args[2];
            System.out.println(cluster.createTopic(topicId, eventType));
            break;
        case "partition":
            if (args.length > 3) {
                throw new IllegalArgumentException("Too many arguments");
            }

            String topicId1 = args[1];
            if (cluster.getTopic(topicId1) == null) {
                throw new IllegalArgumentException("TopicId not found");
            }

            String partitionId = args[2];
            System.out.println(cluster.createPartition(partitionId, topicId1));
            break;
        case "consumer":
            createConsumerOrGroup(Arrays.copyOfRange(args, 1, args.length));
            break;
        case "producer":
            if (args.length > 4) {
                throw new IllegalArgumentException("Too many arguments");
            }

            String producerId = args[1];
            String eventType1 = args[2];
            String allocation = args[3];
            System.out.println(cluster.createProducer(producerId, eventType1, allocation));
            break;
        default:
            break;
        }
    }

    public void produce(String[] args) throws Exception {
        if (args.length < 4 || args.length > 5) {
            throw new IllegalArgumentException("Incorrect arguments");
        }

        String producerId = args[1];
        if (cluster.getProducer(producerId) == null) {
            throw new IllegalArgumentException("ProducerId not found");
        }

        String topicId = args[2];
        if (cluster.getTopic(topicId) == null) {
            throw new IllegalArgumentException("TopicId not found");
        }

        String partitionId = "";
        if (args.length == 5) {
            partitionId = args[4];
            if (cluster.getPartition(partitionId) == null) {
                throw new IllegalArgumentException("PartitionId not found");
            }
        }

        String eventFile = args[3];
        JSONObject eventJson = getJsonObjectFromFile(eventFile);

        String eventType = eventJson.getString("type");
        if (!eventType.equals(cluster.getType("Topic", topicId))) {
            throw new IllegalArgumentException("Event type is different to Topic type");
        }
        if (!eventType.equals(cluster.getType("Producer", producerId))) {
            throw new IllegalArgumentException("Event type is different to Producer type");
        }

        System.out.println(cluster.createEvent(producerId, topicId, partitionId, eventJson));
    }

    private JSONObject getJsonObjectFromFile(String fileName) throws Exception {
        String eventFile = String.format("/events/%s.json", fileName);
        String fileString = new String(getClass().getResourceAsStream(eventFile).readAllBytes());
        JSONObject jsonObject = new JSONObject(fileString);
        return jsonObject;
    }

    public void show(String[] args) throws Exception {
        switch (args[0]) {
        case "topic":
            String topicId = args[1];
            if (cluster.getTopic(topicId) == null) {
                throw new IllegalArgumentException("TopicId not found");
            }
            System.out.println(cluster.showTopic(topicId));
            break;
        case "consumer":
            String groupId = args[2];
            if (cluster.getConsumerGroup(groupId) == null) {
                throw new IllegalArgumentException("ConsumerGroupId not found");
            }
            System.out.println(cluster.showGroup(groupId));
            break;
        default:
            break;
        }
    }

    private void playback(String[] args) throws Exception {
        String consumerId = args[0];
        if (cluster.getConsumer(consumerId) == null) {
            throw new IllegalArgumentException("ConsumerId not found");
        }
        String partitionId = args[1];
        if (cluster.getPartition(partitionId) == null) {
            throw new IllegalArgumentException("PartitionId not found");
        }

        int offset = Integer.parseInt(args[2]);
        System.out.println(cluster.playbackEvents(consumerId, partitionId, offset));

    }

    public void consume(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("Incorrect arguments");
        }

        String consumerId = args[1];
        if (cluster.getConsumer(consumerId) == null) {
            throw new IllegalArgumentException("ConsumerId not found");
        }

        String partitionId = args[2];
        if (cluster.getPartition(partitionId) == null) {
            throw new IllegalArgumentException("PartitionId not found");
        }

        switch (args[0]) {
        case "event":
            System.out.println(cluster.consumeEvent(consumerId, partitionId));
            break;
        case "events":
            int numberEvents = Integer.parseInt(args[3]);
            if (numberEvents < 0) {
                throw new IllegalArgumentException("Number of events must be 0 or greater");
            }
            System.out.println(cluster.consumeEvents(numberEvents, consumerId, partitionId));
        default:
            break;
        }
    }

    private boolean command(String[] args) throws Exception {
        boolean running = true;

        if (args.length < 3) {
            throw new IllegalArgumentException("Not enough arguments");
        }

        switch (args[0]) {
        case "create":
            create(Arrays.copyOfRange(args, 1, args.length));
            break;
        case "delete":
            // deletes consumer
            if (args.length != 3) {
                throw new IllegalArgumentException("Incorrect arguments");
            }

            String consumerId = args[2];
            if (cluster.getConsumer(consumerId) == null) {
                throw new IllegalArgumentException("ConsumerId not found");
            }

            System.out.println(cluster.deleteConsumer(consumerId));
            break;
        case "produce":
            // produces event
            produce(Arrays.copyOfRange(args, 1, args.length));
            break;
        case "consume":
            // consumes a consumer or group
            consume(Arrays.copyOfRange(args, 1, args.length));
            break;
        case "show":
            // show a topic or group
            show(Arrays.copyOfRange(args, 1, args.length));
            break;
        case "set":
            // set a consumer groups rebalancing
            if (args.length != 6) {
                throw new IllegalArgumentException("Incorrect arguments");
            }

            String groupId = args[4];
            if (cluster.getConsumerGroup(groupId) == null) {
                throw new IllegalArgumentException("GroupId not found");
            }

            String rebalancing = args[5];
            if (!rebalancing.equals("Range") && !rebalancing.equals("RoundRobin")) {
                throw new IllegalArgumentException("Invalid rebalancing method");
            }

            System.out.println(cluster.setRebalance(groupId, rebalancing));
            break;
        case "playback":
            // play back events from offset for consumer
            playback(Arrays.copyOfRange(args, 1, args.length));
            break;
        default:
            System.err.println("Parsing error");
            break;
        }
        System.out.println();
        return running;
    }

    public boolean command(String commandString) throws Exception {
        String[] args = commandString.split(" ");
        System.out.println();
        return command(args);
    }

    public void run() throws Exception {
        System.out.print("Enter command: ");
        Scanner scanner = new Scanner(System.in);

        while (command(scanner.nextLine())) {
            System.out.print("Enter command: ");
        }

        scanner.close();
    }

    public static void main(String[] args) {
        TributaryCLI client = new TributaryCLI();

        try {
            client.run();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
