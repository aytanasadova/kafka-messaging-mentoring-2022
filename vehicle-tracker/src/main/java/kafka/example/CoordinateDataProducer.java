package kafka.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.example.exception.KafkaOperationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.apache.commons.lang3.Validate.notBlank;
import static org.apache.commons.lang3.Validate.notNull;

public class CoordinateDataProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinateDataProducer.class);

    private String topicName;
    private KafkaProducer<Integer, String> producer;

    ObjectMapper objectMapper=new ObjectMapper();

    public CoordinateDataProducer(String topicName, KafkaProducer<Integer, String> producer) {
        this.topicName = notBlank(topicName, "topic name is required");
        this.producer = notNull(producer, "producer is required");
    }

    public void send(CoordinateData coordinateData) {
        ProducerRecord<Integer, String> record = null;
        try {
            record = new ProducerRecord<>(topicName, coordinateData.getVehicleId(), objectMapper.writeValueAsString(coordinateData));
            LOGGER.info("sending a sentence: " + coordinateData.toString());
            producer.send(record).get();

        } catch (Exception e) {
            throw new KafkaOperationException("failed to send record", e);
        }
    }
}
