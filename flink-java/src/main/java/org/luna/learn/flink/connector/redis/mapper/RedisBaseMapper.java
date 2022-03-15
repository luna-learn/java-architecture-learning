package org.luna.learn.flink.connector.redis.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class RedisBaseMapper implements RedisMapper, Serializable {
    private static final RedisCommand[] OPTIONAL_COMMAND = {
            RedisCommand.GET,
            RedisCommand.HGET};

    protected RedisCommand redisCommand;
    protected String additionalKey;

    protected String primaryKey;
    protected String[] fieldNames;
    protected TypeInformation<?>[] fieldTypes;
    protected DataType[] dataTypes;

    protected RedisBaseMapper() {

    }

    @Override
    public RedisCommand getCommand() {
        return redisCommand;
    }

    @Override
    public Integer getKeyTtl() {
        return null;
    }

    @Override
    public String getAdditionalKey() {
        return additionalKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    public DataType[] getDataTypes() {
        return dataTypes;
    }

    public Set<RedisCommand> getOptionalCommand() {
        return Arrays.stream(OPTIONAL_COMMAND).collect(Collectors.toSet());
    }
}
