package io.github.sunleader1997.reactorstream.abs;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.HashMap;

public class WorkSpaceEnv extends HashMap<String, Object> {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(100);
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
        this("DEF", 1, Schedulers.DEFAULT_POOL_SIZE,DEFAULT_TIMEOUT);
    }

    public WorkSpaceEnv(String poolName) {
        this(poolName, 1, Schedulers.DEFAULT_POOL_SIZE,DEFAULT_TIMEOUT);
    }

    public WorkSpaceEnv(String poolName, int dequeueSize, int consumerSize, Duration waitDuration) {
        this.poolName = poolName;
        this.dequeueScheduler = Schedulers.newBoundedElastic(dequeueSize, 10, poolName + "-DEQUEUE");
        // Schedulers.newParallel 固定线程数量轮询数据 CPU 密集型计算、并行处理
        // Schedulers.boundedElastic 阻塞型 线程用完后缓存复用 如数据库、HTTP、文件读写
        this.consumerScheduler = Schedulers.newParallel(poolName + "-CONSUMER", consumerSize);
        this.waitDuration = waitDuration;
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
}