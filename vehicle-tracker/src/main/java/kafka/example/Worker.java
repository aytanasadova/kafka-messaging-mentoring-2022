package kafka.example;

import kafka.example.exception.ThreadCommunicationException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.apache.commons.lang3.Validate.notNull;

public class Worker {

    private final ExecutorService executor;
    private CountDownLatch stopPageLatch;

    private volatile boolean stopped;

    public Worker(ExecutorService executor) {
        this.executor = executor;
        stopped=true;
    }

    public void start(Runnable iterationLogic) {
        notNull(iterationLogic, "iteration logic is required");

        if (!stopped) {
            throw new IllegalStateException("the worker is already started");
        }

        stopPageLatch = new CountDownLatch(1);
        stopped = false;
        executor.submit(() -> run(iterationLogic));
    }
    private void run(Runnable iterationLogic) {
        while (!stopped) {
            iterationLogic.run();
        }

        stopPageLatch.countDown();
    }

    public void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        try {
            stopPageLatch.await();
        } catch (InterruptedException e) {
            throw new ThreadCommunicationException("failed to wait for a worker to stop", e);
        }
    }
}
