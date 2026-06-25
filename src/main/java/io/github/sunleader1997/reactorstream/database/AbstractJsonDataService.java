package io.github.sunleader1997.reactorstream.database;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.TypeReference;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractJsonDataService<T extends AbstractData> {

    private final File dbFile;
    private final Map<String, T> dataMap;

    /**
     * @param path example: file:db/data.json
     */
    public AbstractJsonDataService(String path) {
        this.dataMap = new ConcurrentHashMap<>();
        try {
            URL url = ResourceUtils.toURL(path);
            this.dbFile = new File(url.getFile());
            if (!dbFile.exists()) {
                FileUtil.touch(dbFile);
            }
            loadData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void loadData() {
        String contentOnFile = FileUtil.readUtf8String(dbFile);
        String content = StrUtil.blankToDefault(contentOnFile, "[]");
        List<T> list = new TypeReference<T>() {
        }.parseArray(content);
        this.dataMap.clear();
        list.forEach(item -> {
            this.dataMap.put(item.getId().toString(), item);
        });
    }

    protected void dump() {
        String json = JSON.toJSONString(this.dataMap.values(), JSONWriter.Feature.PrettyFormat);
        FileUtil.writeUtf8String(json, dbFile);
    }

    public void saveOrUpdate(T data) {
        data.generateIdIfEmpty();
        this.dataMap.put(data.getId().toString(), data);
        dump();
    }

    public T get(Serializable id) {
        return this.dataMap.get(id.toString());
    }

    public boolean remove(Serializable id) {
        return this.dataMap.remove(id.toString()) != null;
    }

    public List<T> getAll() {
        return this.dataMap.values().stream().toList();
    }
}
