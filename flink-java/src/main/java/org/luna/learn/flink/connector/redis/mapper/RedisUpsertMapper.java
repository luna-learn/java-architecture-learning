package org.luna.learn.flink.connector.redis.mapper;

import com.sun.istack.internal.NotNull;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.luna.learn.flink.connector.redis.UnsupportedRedisException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisUpsertMapper implements RedisMapper, Serializable {
    private static final RedisCommand[] OPTIONAL_COMMAND = {
            RedisCommand.SET,
            RedisCommand.HSET};

    private RedisCommand redisCommand;
    private String additionalKey;

    private RedisFormatter<?> redisFormatter;
    private String primaryKey;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    private RedisUpsertMapper() {

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

    @Override
    public RedisFormatter<?> getFormatter() {
        return redisFormatter;
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

    public Set<RedisCommand> getOptionalCommand() {
        return Arrays.stream(OPTIONAL_COMMAND).collect(Collectors.toSet());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        RedisUpsertMapper mapper;
        private Builder() {
            mapper = new RedisUpsertMapper();
            mapper.redisCommand = RedisCommand.HSET;
        }

        public Builder setRedisCommand(@NotNull RedisCommand redisCommand) {
            if (Arrays.asList(OPTIONAL_COMMAND).contains(redisCommand)) {
                mapper.redisCommand = redisCommand;
            } else {
                throw new UnsupportedRedisException("RedisMapper unsupported" +
                        " redis command " + redisCommand);
            }
            return this;
        }

        public Builder setAdditionalKey(String additionalKey) {
            mapper.additionalKey = additionalKey;
            return this;
        }

        public Builder setRedisFormatter(RedisFormatter<?> redisFormatter) {
            mapper.redisFormatter = redisFormatter;
            return this;
        }

        public Builder setPrimaryKey(String primaryKey) {
            mapper.primaryKey = primaryKey;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            mapper.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation<?>[] fieldTypes) {
            mapper.fieldTypes = fieldTypes;
            return this;
        }

        public RedisUpsertMapper build() {
            return mapper;
        }

    }
}
