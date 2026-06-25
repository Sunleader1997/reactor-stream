package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.AbsPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class PipelineManager {

    /**
     * key: id
     * value: pipeline
     */
    private static final ConcurrentHashMap<String, AbsPipeline> PIPELINE_CACHE = new ConcurrentHashMap<>();
    private static final Logger log = LoggerFactory.getLogger(PipelineManager.class);

    /**
     * 保存，重复ID会触发销毁
     * @param absPipeline
     * @param <T>
     */
    public static <T extends AbsPipeline> void save(T absPipeline) {
        if (absPipeline == null || absPipeline.getId() == null || "".equals(absPipeline.getId())) return;
        get(absPipeline.getId()).ifPresent(duplicate -> {
            log.warn("Duplicate pipeline id: {}", duplicate.getId());
            remove(absPipeline.getId());
        });
        PIPELINE_CACHE.put(absPipeline.getId(), absPipeline);
    }

    /**
     * 获取
     */
    public static <T extends AbsPipeline> Optional<T> get(String id) {
        if (id == null || "".equals(id)) return Optional.empty();
        if (!PIPELINE_CACHE.containsKey(id)) return Optional.empty();
        return Optional.of((T) PIPELINE_CACHE.get(id));
    }

    /**
     * 移除缓存并销毁
     */
    public static <T extends AbsPipeline> void remove(String id) {
        if (id == null || "".equals(id)) return;
        log.info("Removing pipeline id: {}", id);
        AbsPipeline absPipeline = PIPELINE_CACHE.remove(id);
        if (absPipeline != null) {
            try {
                log.info("Close pipeline id: {}", id);
                absPipeline.close();
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
    }
}
