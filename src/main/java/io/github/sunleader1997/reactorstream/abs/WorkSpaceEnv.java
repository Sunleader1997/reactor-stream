package io.github.sunleader1997.reactorstream.abs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class WorkSpaceEnv implements AutoCloseable {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(100);
    private static final Logger log = LoggerFactory.getLogger(WorkSpaceEnv.class);
    /**
     * 名称
     */
    private final String poolName;
    /**
     * 拉取线程
     */
    private final Scheduler dequeueScheduler;
    /**
     * 消费线程
     */
    private final Scheduler consumerScheduler;

    /**
     * pull 如果拉到空数据需要等待，不然CPU空转
     */
    private final Duration waitDuration;


    public WorkSpaceEnv() {
        this("DEF", Schedulers.DEFAULT_POOL_SIZE, DEFAULT_TIMEOUT);
    }

    public WorkSpaceEnv(String poolName) {
        this(poolName, Schedulers.DEFAULT_POOL_SIZE, DEFAULT_TIMEOUT);
    }

    public WorkSpaceEnv(String poolName, int consumerSize, Duration waitDuration) {
        this.poolName = poolName;
        this.dequeueScheduler = Schedulers.newSingle(poolName + "-DEQUEUE");
        // Schedulers.newParallel 固定线程数量轮询数据 CPU 密集型计算、并行处理
        // Schedulers.boundedElastic 阻塞型 线程用完后缓存复用 如数据库、HTTP、文件读写
        this.consumerScheduler = Schedulers.newParallel(poolName + "-CONSUMER", consumerSize);
        this.waitDuration = waitDuration;
        log.info("WorkSpaceEnv initialized [Consumer:{}]", consumerSize);
    }

    public String getPoolName() {
        return poolName;
    }

    public Scheduler getDequeueScheduler() {
        return dequeueScheduler;
    }

    public Scheduler getConsumerScheduler() {
        return consumerScheduler;
    }

    public Duration getWaitDuration() {
        return waitDuration;
    }

    /**
     * 销毁调度器，释放线程池资源
     */
    @Override
    public void close() {
        if (dequeueScheduler != null && !dequeueScheduler.isDisposed()) {
            dequeueScheduler.dispose();
        }
        if (consumerScheduler != null && !consumerScheduler.isDisposed()) {
            consumerScheduler.dispose();
        }
    }
}