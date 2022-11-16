package kafka.example;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.StreamSupport;

import static org.apache.commons.lang3.Validate.notNull;

public class LoggerConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerConsumer.class);

    private Worker worker;
    private KafkaConsumer<String, Integer> kafkaConsumer;
    private Duration pollingDuration;

    private BlockingQueue<Pair<Integer, String>> recordBuffer;

    public LoggerConsumer(Worker worker, KafkaConsumer<String, Integer> kafkaConsumer, Duration pollingDuration) {
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
            .peek(pair -> writeLog(pair));
//            .forEach(recordBuffer::offer);
    }

    public void stop() {
        worker.stop();
    }

    private void writeLog(ImmutablePair<String, Integer> pair){
        LOGGER.info("got stats: {" + pair.getKey() + "=" + pair.getValue() + "}");
    }


//    public Map<String, Integer> getData() {
//        var list = new ArrayList<Pair<String, Integer>>();
//        recordBuffer.drainTo(list);
//
//        var map = new HashMap<String, Integer>();
//        list.forEach(pair -> map.put(pair.getKey(), pair.getValue()));
//
//        return map;
//    }

}
