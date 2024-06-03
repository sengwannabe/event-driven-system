package tributary.core;

import tributary.api.API;

public class ProducerFactory {
    public Producer<?> makeRandomProducer(String producerId, API cluster, String type) {
        switch (type) {
        case "String":
            return new RandomProducer<String>(producerId, type, cluster);
        case "Integer":
            return new RandomProducer<Integer>(producerId, type, cluster);
        default:
            return null;
        }
    }

    public Producer<?> makeManualProducer(String producerId, API cluster, String type) {
        switch (type) {
        case "String":
            return new ManualProducer<String>(producerId, type, cluster);
        case "Integer":
            return new ManualProducer<Integer>(producerId, type, cluster);
        default:
            return null;
        }
    }
}
