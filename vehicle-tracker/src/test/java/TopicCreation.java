import static org.apache.commons.lang3.Validate.notBlank;

public record TopicCreation(String inputTopicName, String outputTopicName) {

    public TopicCreation(String inputTopicName, String outputTopicName) {
        this.inputTopicName = notBlank(inputTopicName, "input topic name is required");
        this.outputTopicName = notBlank(outputTopicName, "output topic name is required");
    }

}
