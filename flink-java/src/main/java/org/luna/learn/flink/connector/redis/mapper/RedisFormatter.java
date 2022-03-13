package org.luna.learn.flink.connector.redis.mapper;

public interface RedisFormatter<OUT> {

    String getFormatterType();

    OUT accept(String value);
}
