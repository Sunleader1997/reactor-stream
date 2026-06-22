package io.github.sunleader1997.reactorstream.abs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkSpaceEnv implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkSpaceEnv.class);
    /**
     * 名称
     */
    private final String poolName;
    /**
     * 消费线程
     */
    private final Scheduler consumerScheduler;
    private final int consumerSize;


    public WorkSpaceEnv() {
        this("DEF", Schedulers.DEFAULT_POOL_SIZE);
    }

    public WorkSpaceEnv(boolean singleThread) {
        this("DEF", singleThread ? 0 : Schedulers.DEFAULT_POOL_SIZE);
    }

    public WorkSpaceEnv(String poolName) {
        this(poolName, Schedulers.DEFAULT_POOL_SIZE);
    }

    /**
     * @param consumerSize <0 单线程执行
     */
    public WorkSpaceEnv(String poolName, int consumerSize) {
        this.poolName = poolName;
        this.consumerSize = consumerSize;
        if (consumerSize > 0) {
            // Schedulers.newParallel 固定线程数量轮询数据 CPU 密集型计算、并行处理
            // Schedulers.boundedElastic 阻塞型 线程用完后缓存复用 如数据库、HTTP、文件读写
            this.consumerScheduler = Schedulers.newParallel(poolName + "-CONSUMER", consumerSize);
        } else {
            this.consumerScheduler = null;
        }
        log.info("WorkSpaceEnv initialized [Consumer:{}]", consumerSize);
    }

    public String getPoolName() {
        return poolName;
    }

    public Scheduler getConsumerScheduler() {
        return consumerScheduler;
    }

    public int getConsumerSize() {
        return consumerSize;
    }

    /**
     * 销毁调度器，释放线程池资源
     */
    @Override
    public void close() {
        if (consumerScheduler != null && !consumerScheduler.isDisposed()) {
            consumerScheduler.dispose();
        }
    }
}