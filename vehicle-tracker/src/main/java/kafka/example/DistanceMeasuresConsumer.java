package kafka.example;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.StreamSupport;

import static org.apache.commons.lang3.Validate.notNull;

public class DistanceMeasuresConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistanceMeasuresConsumer.class);

    private Worker worker;
    private KafkaConsumer<Integer, Double> kafkaConsumer;
    private Duration pollingDuration;

    private BlockingQueue<Pair<Integer, Double>> recordBuffer;

    public DistanceMeasuresConsumer(Worker worker, KafkaConsumer<Integer, Double> kafkaConsumer, Duration pollingDuration) {
        this.worker = notNull(worker, "worker is required");
        this.kafkaConsumer = notNull(kafkaConsumer, "kafka consumer is required");
        this.pollingDuration = notNull(pollingDuration, "polling duration is required");

        recordBuffer = new LinkedBlockingDeque<>();
    }

    public void start() {
        LOGGER.info("starting word count consumption");
        worker.start(this::consumeBatch);
    }

    private void consumeBatch() {
        StreamSupport.stream(kafkaConsumer.poll(pollingDuration).spliterator(), false)
                .map(record -> new ImmutablePair<>(record.key(), record.value()))
                .peek(pair -> LOGGER.info("got stats: {" + pair.getKey() + "=" + pair.getValue() + "}"))
                .forEach(recordBuffer::offer);
    }

    public void stop() {
        worker.stop();
    }



    public List<DistanceData> getData() {
        var list = new ArrayList<Pair<Integer, Double>>();
        recordBuffer.drainTo(list);
        var map = new HashMap<Integer, Double>();
      List<DistanceData> distanceData=new ArrayList<>();
        list.forEach(pair -> distanceData.add(new DistanceData(pair.getKey(),pair.getValue())));
//        list.forEach(pair ->  map.put(pair.getKey(), pair.getValue()));
        return distanceData;
    }

//    public Map<Integer, Double> getData() {
//        var list = new ArrayList<Pair<Integer, Double>>();
//        recordBuffer.drainTo(list);
//        var map = new HashMap<Integer, Double>();
//        list.forEach(pair -> map.put(pair.getKey(), pair.getValue()));
//        return map;
//    }

}
