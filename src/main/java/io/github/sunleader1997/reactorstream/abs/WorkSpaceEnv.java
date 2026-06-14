package io.github.sunleader1997.reactorstream.abs;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;

public class WorkSpaceEnv extends HashMap<String, Object> {
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


    public WorkSpaceEnv() {
        this("DEF", 1, Schedulers.DEFAULT_POOL_SIZE);
    }

    public WorkSpaceEnv(String poolName) {
        this(poolName, 1, Schedulers.DEFAULT_POOL_SIZE);
    }

    public WorkSpaceEnv(String poolName, int dequeueSize, int consumerSize) {
        this.poolName = poolName;
        if (dequeueSize == 1) {
            this.dequeueScheduler = Schedulers.newSingle(poolName + "-DEQUEUE");
        } else {
            this.dequeueScheduler = Schedulers.newBoundedElastic(dequeueSize, 0, poolName + "-DEQUEUE");
        }
        // Schedulers.newParallel 固定线程数量轮询数据 CPU 密集型计算、并行处理
        // Schedulers.boundedElastic 阻塞型 线程用完后缓存复用 如数据库、HTTP、文件读写
        this.consumerScheduler = Schedulers.newParallel(poolName + "-CONSUMER", consumerSize);
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
}