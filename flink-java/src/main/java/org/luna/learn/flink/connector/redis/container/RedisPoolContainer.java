package org.luna.learn.flink.connector.redis.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.luna.learn.flink.connector.redis.UnsupportedRedisException;
import org.luna.learn.flink.connector.redis.config.RedisConnectorOptions;
import org.luna.learn.flink.connector.redis.config.RedisOptions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.util.Pool;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisPoolContainer implements RedisContainer, Closeable {

    private final RedisConnectorOptions options;
    private JedisPool jedisPool;
    private JedisSentinelPool sentinelPool;
    private Jedis jedis;

    public RedisPoolContainer(RedisConnectorOptions options) {
        this.options = options;
    }

    private Jedis getResource() {
        if (jedis == null) {
            switch (options.getMode()) {
                case RedisOptions.MODE_SINGLE:
                    jedis = jedisPool.getResource();
                    break;
                case RedisOptions.MODE_SENTINEL:
                    jedis = sentinelPool.getResource();
                    break;
                default:
                    throw new UnsupportedRedisException("RedisPoolContainer unsupported " +
                            "redis mode " + options.getMode());
            }
        }
        return jedis;
    }

    @Override
    public void open() throws Exception {
        if (jedisPool != null || sentinelPool != null) {
            return;
        }
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(options.getMaxTotal());
        poolConfig.setMaxIdle(options.getMaxIdle());
        poolConfig.setMinIdle(options.getMinIdle());
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);

        switch(options.getMode()) {
            case RedisOptions.MODE_SINGLE:
                jedisPool = new JedisPool(poolConfig,
                        options.getHost(),
                        options.getPort(),
                        options.getTimeout(),
                        options.getPassword(),
                        options.getDatabase());
                break;
            case RedisOptions.MODE_SENTINEL:
                Set<String> sentinelNodes = Arrays
                        .stream(options.getSentinelNodes().split(","))
                        .collect(Collectors.toSet());
                sentinelPool = new JedisSentinelPool(options.getSentinelMaster(),
                        sentinelNodes,
                        poolConfig,
                        options.getSoTimeout(),
                        options.getTimeout(),
                        options.getPassword(),
                        options.getDatabase());
                break;
            default:
                throw new UnsupportedRedisException("RedisPoolContainer unsupported " +
                        "redis mode " + options.getMode());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.returnResource(jedis);
        }
        if (sentinelPool != null) {
            sentinelPool.returnResource(jedis);
        }
        jedis = null;
    }

    @Override
    public void set(String key, String value) {
        getResource().set(key, value);
    }

    @Override
    public String get(String key) {
        return getResource().get(key);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
        return  getResource().hscan(key, cursor, params);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return  getResource().hscan(key, cursor);
    }


    @Override
    public void hset(String key, String field, String value) {
        getResource().hset(key, field, value);
    }

    @Override
    public void hset(String key, String field, String value, int ttl) {

    }

    @Override
    public String hget(String key, String field) {
        return getResource().hget(key, field);
    }

    @Override
    public Set<String> keys(String pattern) {
        return getResource().keys(pattern);
    }

    @Override
    public Set<String> hkeys(String key) {
        return getResource().hkeys(key);
    }

    @Override
    public String type(String key) {
        return getResource().type(key);
    }
}
