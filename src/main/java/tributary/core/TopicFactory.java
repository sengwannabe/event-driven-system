package tributary.core;

public class TopicFactory {
    public Topic<?> makeTopic(String id, String type) {
        switch (type) {
        case "String":
            return new Topic<String>(id, type);
        case "Integer":
            return new Topic<Integer>(id, type);
        default:
            return null;
        }
    }

}
