package kafka.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.example.exception.KafkaOperationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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

import static java.util.stream.Collectors.toMap;

public class DistanceCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistanceCalculator.class);
    private final String resultTopicName;
    private final KafkaProducer<Integer, Double> distanceMeasuresProducer;
    private final KafkaConsumer<Integer, String> coordinateDataConsumer;
    private final Duration pollingDuration;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Map<Integer, CoordinateData> lastKnownCoordinatesOfVehicles=new HashMap<>();

    public DistanceCalculator(String resultTopicName, KafkaProducer<Integer, Double> distanceMeasuresProducer, KafkaConsumer<Integer, String> coordinateDataConsumer, Duration pollingDuration) {

        this.resultTopicName = resultTopicName;
        this.distanceMeasuresProducer = distanceMeasuresProducer;
        this.coordinateDataConsumer = coordinateDataConsumer;
        this.pollingDuration = pollingDuration;
    }


    public void processSentenceBatch() {
        var dataToSend = measureDistance();

        if (dataToSend.isEmpty()) {
            LOGGER.info("nothing to submit");
            return;
        }
        LOGGER.info("submitting stats: " + dataToSend.distances());
        inTransaction(() -> dataToSend.distances().forEach(this::send), dataToSend.offsets());
    }

    private void send(DistanceData distanceData) {
        var record = new ProducerRecord<>(resultTopicName, distanceData.getVehicleId(), distanceData.getDistance());
        try {
            distanceMeasuresProducer.send(record).get();
        } catch (Exception e) {
            throw new KafkaOperationException("failed to send a record", e);
        }
    }

    private CoordinateData mapJsonToCoordinateDataObject(String json) {
        try {
            return objectMapper.readValue(json, CoordinateData.class);
        } catch (IOException e) {
//            throw new RuntimeException(e);
        }
        return null;
    }



    private   DistanceData calculateDistance(CoordinateData currentCoordinateData) {
        CoordinateData lastCoordinateData = new CoordinateData();
        if (lastKnownCoordinatesOfVehicles.containsKey(currentCoordinateData.getVehicleId())) {
            lastCoordinateData = lastKnownCoordinatesOfVehicles.get(currentCoordinateData.getVehicleId());
            double ac = Math.abs(currentCoordinateData.getLongitude() - lastCoordinateData.getLongitude());
            double cb = Math.abs(currentCoordinateData.getLatitude() - lastCoordinateData.getLatitude());
            lastKnownCoordinatesOfVehicles.put(currentCoordinateData.getVehicleId(), currentCoordinateData);
            return new DistanceData(currentCoordinateData.getVehicleId(), Math.hypot(ac, cb));
        } else {
            lastKnownCoordinatesOfVehicles.put(currentCoordinateData.getVehicleId(),currentCoordinateData);
            return null;
        }

    }

    private SendStageInput measureDistance() {

        ConsumerRecords<Integer, String> records = coordinateDataConsumer.poll(pollingDuration);

        var coordinateDataList = StreamSupport.stream(records.spliterator(), false)
                .map(ConsumerRecord::value).filter(Objects::nonNull)
                .peek(coordinate -> LOGGER.info("processing coordinate data: " + coordinate))
                .map(coordinateJson -> mapJsonToCoordinateDataObject(coordinateJson)).filter(Objects::nonNull).collect(Collectors.toList());


        var distances = coordinateDataList.stream()
                .map(coordinateData -> calculateDistance(coordinateData))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());


        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        records.partitions().forEach(partition -> {
            var partitionedRecords = records.records(partition);
            var offset = partitionedRecords.get(partitionedRecords.size() - 1).offset() + 1;
            offsets.put(partition, new OffsetAndMetadata(offset));
        });
        return new SendStageInput(distances, offsets);
    }



    private void inTransaction(Runnable logic, Map<TopicPartition, OffsetAndMetadata> offsets) {
        distanceMeasuresProducer.beginTransaction();
        try {
            logic.run();
        } catch (KafkaOperationException e) {
            distanceMeasuresProducer.abortTransaction();
            return;
        }
        distanceMeasuresProducer.sendOffsetsToTransaction(offsets, coordinateDataConsumer.groupMetadata());
        distanceMeasuresProducer.commitTransaction();
    }

    private record SendStageInput(List<DistanceData> distances, Map<TopicPartition, OffsetAndMetadata> offsets) {
        public boolean isEmpty() {
            return distances.isEmpty();
        }
    }
}
