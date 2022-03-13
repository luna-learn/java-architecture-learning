package org.luna.learn.flink.connector.redis.container;

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisContainer {

    void open() throws Exception;

    void close();

    void set(String key, String value);

    String get(String key);

    ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params);

    default ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return hscan(key, cursor, new ScanParams());
    }

    void hset(String key, String field, String value);

    void hset(String key, String field, String value, int ttl);

    String hget(String key, String field);


    Set<String> keys(String pattern);

    Set<String> hkeys(String key);

    String type(String key);
}
