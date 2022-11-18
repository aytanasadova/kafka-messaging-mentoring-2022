package kafka.example;

import static org.apache.commons.lang3.Validate.notNull;

public class DistanceCalculatorService {
    private DistanceCalculator distanceCalculator;
    private Worker worker;

    public DistanceCalculatorService(DistanceCalculator distanceCalculator, Worker worker) {
        this.distanceCalculator = notNull(distanceCalculator, "counter is required");
        this.worker = notNull(worker, "worker is required");

    }

    public void run() {
        worker.start(distanceCalculator::processSentenceBatch);
    }

    public void stop() {
        worker.stop();
    }
}
