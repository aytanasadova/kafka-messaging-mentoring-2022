package kafka.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.commons.lang3.Validate.notBlank;
import static org.apache.commons.lang3.Validate.notNull;

public class VehicleTracker {


    private static final Logger LOGGER = LoggerFactory.getLogger(VehicleTracker.class);
    private final Map<Integer, VehicleCoordinates> vehicleLastState = new HashMap<>();
    private final String resultTopicName;
    private final KafkaProducer<Integer, String> resultsProducer;
    private final KafkaConsumer<String, String> coordinateConsumer;
    private final Duration pollingDuration;
    ObjectMapper objectMapper = new ObjectMapper();
    private KafkaConsumer<String, String> vehicleCoordinateConsumer;

    public VehicleTracker(String resultTopicName, KafkaProducer<Integer, String> resultsProducer, KafkaConsumer<String, String> coordinateConsumer, Duration pollingDuration) {
        this.resultTopicName = notBlank(resultTopicName, "result topic name is required");
        this.resultsProducer = notNull(resultsProducer, "results producer is required");
        this.coordinateConsumer = notNull(coordinateConsumer, "sentence consumer is required");
        this.pollingDuration = notNull(pollingDuration, "polling duration is required");
    }

    public void processSentenceBatch() {
        var dataToSend = getDistanceData();
        if (dataToSend.isEmpty()) {
            LOGGER.info("nothing to submit");
            return;
        }
        LOGGER.info("submitting stats: " + dataToSend.travelledDistanceList());
        inTransaction(() -> dataToSend.travelledDistanceList().forEach(this::send), dataToSend.offsets());
    }


    private void inTransaction(Runnable logic, Map<TopicPartition, OffsetAndMetadata> offsets) {
        resultsProducer.beginTransaction();

        try {
            logic.run();
        } catch (KafkaOperationException e) {
            resultsProducer.abortTransaction();
            return;
        }

        resultsProducer.sendOffsetsToTransaction(offsets, coordinateConsumer.groupMetadata());
        resultsProducer.commitTransaction();
    }

    private TravelledDistance calculateDistance(int vehicleId, double long1, double lat1, double long2, double lat2) {
        double ac = Math.abs(lat2 - lat1);
        double cb = Math.abs(long2 - long1);

        return new TravelledDistance(vehicleId, Math.hypot(ac, cb));
    }


    private VehicleCoordinates getLastCoordinatesByVehicle(int vehicleId) {
        return vehicleLastState.get(vehicleId);
    }


    private TravelledDistance getTravelledDistanceByVehicle(@NonNull VehicleCoordinates currentCoordinates) {
        var vehicleId = currentCoordinates.getVehicleId();
        var lastCoordinates = getLastCoordinatesByVehicle(vehicleId);


        vehicleLastState.put(vehicleId, currentCoordinates);
       return calculateDistance(vehicleId, currentCoordinates.getLatitude(), currentCoordinates.getLongitude(), lastCoordinates.getLatitude(), lastCoordinates.getLongitude());

    }


    private SendStageInput1 getDistanceData() {
        ConsumerRecords<String, String> records = vehicleCoordinateConsumer.poll(pollingDuration);

        List<VehicleCoordinates> vehicleCoordinates = StreamSupport.stream(records.spliterator(), false).map(ConsumerRecord::value).filter(Objects::nonNull).peek(e -> LOGGER.info("processing coordinate data")).map(json -> mapToObject(json)).collect(Collectors.toList());

        List<TravelledDistance> travelledDistances = vehicleCoordinates.stream().filter(Objects::nonNull)
                .map(e -> getTravelledDistanceByVehicle(e)).collect(Collectors.toList());


        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        records.partitions().forEach(partition -> {
            var partitionedRecords = records.records(partition);
            var offset = partitionedRecords.get(partitionedRecords.size() - 1).offset() + 1;
            offsets.put(partition, new OffsetAndMetadata(offset));
        });

        return new SendStageInput1(travelledDistances, offsets);
    }


    private VehicleCoordinates mapToObject(String json) {
        try {
            return objectMapper.readValue(json, VehicleCoordinates.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void send(TravelledDistance value) {
        ProducerRecord<Integer, String> record = null;
        try {
            record = new ProducerRecord<>(resultTopicName, value.getVehicleId(), objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        try {
            resultsProducer.send(record).get();
        } catch (Exception e) {
            throw new KafkaOperationException("failed to send a record", e);
        }
    }


    private record SendStageInput1(List<TravelledDistance> travelledDistanceList,
                                   Map<TopicPartition, OffsetAndMetadata> offsets) {
        public boolean isEmpty() {
            return travelledDistanceList.isEmpty();
        }

    }

}
