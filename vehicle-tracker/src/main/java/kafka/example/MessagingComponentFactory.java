package kafka.example;

import kafka.example.exception.KafkaOperationException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.apache.commons.lang3.Validate.notBlank;

public class MessagingComponentFactory {//implements DisposableBean {

    private String kafkaBootStrapServerUrl;

    private List<KafkaProducer<?, ?>> producers = new ArrayList<>();
    private List<KafkaConsumer<?, ?>> consumers = new ArrayList<>();

    public MessagingComponentFactory(String kafkaBootStrapServerUrl) {
        this.kafkaBootStrapServerUrl = notBlank(kafkaBootStrapServerUrl, "kafka bootstrap server URL is required");
    }

    public DistanceCalculator createDistanceCalculator(String inputTopicName, String outputTopicName, Duration pollingDuration) {
        var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServerUrl);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "calculator-id");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var coordinateDataConsumer = new KafkaConsumer<>(
            consumerProperties, Serdes.Integer().deserializer(), Serdes.String().deserializer());
        coordinateDataConsumer.subscribe(Collections.singleton(inputTopicName));
        consumers.add(coordinateDataConsumer);

        var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServerUrl);
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // in prod, transaction IDs must be carefully mapped onto partitions!
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test_id");

        var resultsProducer = new KafkaProducer<>(
            producerProperties, Serdes.Integer().serializer(), Serdes.Double().serializer());
        resultsProducer.initTransactions();
        producers.add(resultsProducer);

        return new DistanceCalculator(outputTopicName, resultsProducer, coordinateDataConsumer, pollingDuration);
    }

    public CoordinateDataProducer createProducer(String topicName) {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServerUrl);

        var kafkaProducer = new KafkaProducer<>(properties, Serdes.Integer().serializer(), Serdes.String().serializer());
        producers.add(kafkaProducer);

        return new CoordinateDataProducer(topicName, kafkaProducer);
    }

    public DistanceMeasuresConsumer createConsumer(String topicName, Worker worker, Duration pollingDuration) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServerUrl);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-data-provider");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var kafkaConsumer =
            new KafkaConsumer<>(properties, Serdes.Integer().deserializer(), Serdes.Double().deserializer());
        kafkaConsumer.subscribe(Collections.singleton(topicName));
        consumers.add(kafkaConsumer);

        return new DistanceMeasuresConsumer(worker, kafkaConsumer, pollingDuration);
    }

    public void createTopics(String... names) {
        var kafkaProperties = new Properties();
        kafkaProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServerUrl);

        var admin = AdminClient.create(kafkaProperties);
        try (admin) {
            var requests = Stream.of(names).map(this::newTopic).toList();
            admin.createTopics(requests)
                .all()
                .get();
        } catch (ExecutionException | InterruptedException e) {
            throw new KafkaOperationException("failed to create test topics", e);
        }
    }

    private NewTopic newTopic(String name) {
        return new NewTopic(name, 1, (short) 1);
    }

    public void destroy() {
        producers.forEach(KafkaProducer::close);
        consumers.forEach(consumer -> {
            consumer.unsubscribe();
            consumer.close();
        });
    }

}
