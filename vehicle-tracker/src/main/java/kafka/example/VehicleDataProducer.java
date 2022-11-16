package kafka.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.apache.commons.lang3.Validate.notBlank;
import static org.apache.commons.lang3.Validate.notNull;

public class VehicleDataProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(VehicleDataProducer.class);

    private String topicName;
    private KafkaProducer<String, String> producer;

    public VehicleDataProducer(String topicName, KafkaProducer<String, String> producer) {

        this.topicName = notBlank(topicName, "topic name is required");
        this.producer = notNull(producer, "producer is required");
    }

    public void send(String message) {
        var record = new ProducerRecord<>(topicName, UUID.randomUUID().toString(), message);
        try {
            LOGGER.info("sending a sentence: " + message);
            producer.send(record).get();
        } catch (Exception e) {
            throw new KafkaOperationException("failed to send record", e);
        }
    }

}
