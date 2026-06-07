package alex.exam.schedulers;

import alex.exam.Scheduler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IoScheduler implements Scheduler {
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    public void shutdown() {
        executor.shutdown();
    }
}