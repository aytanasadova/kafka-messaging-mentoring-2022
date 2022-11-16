package kafka.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.apache.commons.lang3.Validate.notNull;

public class Worker {

    private ExecutorService executor;

    private CountDownLatch stoppageLatch;
    private volatile boolean stopped;

    public Worker(ExecutorService executor) {
        this.executor = notNull(executor, "executor is required");
        stopped = true;
    }

    public void start(Runnable iterationLogic) {
        notNull(iterationLogic, "iteration logic is required");

        if (!stopped) {
            throw new IllegalStateException("the worker is already started");
        }

        stoppageLatch = new CountDownLatch(1);
        stopped = false;
        executor.submit(() -> run(iterationLogic));
    }

    private void run(Runnable iterationLogic) {
        while (!stopped) {
            iterationLogic.run();
        }

        stoppageLatch.countDown();
    }

    public void stop() {
        if (stopped) {
            return;
        }

        stopped = true;
        try {
            stoppageLatch.await();
        } catch (InterruptedException e) {
            throw new ThreadCommunicationException("failed to wait for a worker to stop", e);
        }
    }

}
