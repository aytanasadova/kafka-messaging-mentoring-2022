package kafka.example;

import static org.apache.commons.lang3.Validate.notNull;

public class VehicleTrackerService {

    private VehicleTracker counter;
    private Worker worker;

    public VehicleTrackerService(VehicleTracker counter, Worker worker) {
        this.counter = notNull(counter, "counter is required");
        this.worker = notNull(worker, "worker is required");
    }

    public void run() {
        worker.start(counter::processSentenceBatch);
    }

    public void stop() {
        worker.stop();
    }

}
