import kafka.example.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class TestContext {

    @Bean
    public DistanceCalculatorService distanceCalculatorService(MessagingComponentFactory factory, ExecutorService executor,
                                                               TopicCreation topics) {
        var counter = factory.createDistanceCalculator(
            topics.inputTopicName(), topics.outputTopicName(), Duration.ofMillis(500));
        var worker = new Worker(executor);

        return new DistanceCalculatorService(counter, worker);
    }

    @Bean
    public CoordinateDataProducer createProducer(TopicCreation topics, MessagingComponentFactory factory) {
        return factory.createProducer(topics.inputTopicName());
    }

    @Bean
    public DistanceMeasuresConsumer distanceMeasuresConsumer(TopicCreation topics, ExecutorService executor,
                                                      MessagingComponentFactory factory) {
        return factory.createConsumer(topics.outputTopicName(), new Worker(executor), Duration.ofMillis(100));
    }

    @Bean
    public MessagingComponentFactory messagingComponentFactory(
            @Value("${spring.embedded.kafka.brokers}") String kafkaBootstrapServerUrl) {
        return new MessagingComponentFactory(kafkaBootstrapServerUrl);
    }

    @Bean
    public TopicCreation topics(MessagingComponentFactory factory) {
        TopicCreation topics = new TopicCreation("input-topic", "output-topic");

        factory.createTopics(topics.inputTopicName(), topics.outputTopicName());

        return topics;
    }

    @Bean
    public ExecutorService executor() {
        return Executors.newFixedThreadPool(2);
    }

}
