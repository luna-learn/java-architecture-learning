package org.apache.flink.connector.redis.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.connector.redis.UnsupportedRedisException;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisOptions;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisClusterContainer extends RedisBaseContainer{

    private final RedisConnectorOptions options;
    private JedisCluster cluster;

    public RedisClusterContainer(RedisConnectorOptions options) {
        this.options = options;
    }

    private JedisCluster getResource() {
        return cluster;
    }

    @Override
    public void open() throws Exception {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(options.getMaxTotal());
        poolConfig.setMaxIdle(options.getMaxIdle());
        poolConfig.setMinIdle(options.getMinIdle());
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        if (Objects.equals(options.getMode(), RedisOptions.MODE_CLUSTER)) {
            Set<HostAndPort> clusterNodes = Arrays
                    .stream(options.getClusterNodes().split(","))
                    .map(e -> {
                        if (e == null || e.length() == 0) {
                            return null;
                        }
                        int a = e.indexOf(":");
                        if (a == -1) {
                            return new HostAndPort(e, options.getPort());
                        } else {
                            return new HostAndPort(e.substring(0, a), Integer.parseInt(e.substring(a + 1)));
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            cluster = new JedisCluster(clusterNodes,
                    options.getTimeout(),
                    options.getSoTimeout(),
                    3,
                    options.getPassword(),
                    poolConfig);
        } else {
            throw new UnsupportedRedisException("RedisClusterContainer unsupported " +
                    "redis mode " + options.getMode());
        }


    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Override
    protected JedisCommands getCommander() {
        return getResource();
    }

    public void del(String key) {
        getResource().del(key);
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
    public void hdel(String key, String field) {
        getResource().hdel(key, field);
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
