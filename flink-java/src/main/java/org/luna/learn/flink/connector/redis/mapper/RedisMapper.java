package org.luna.learn.flink.connector.redis.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;
import java.util.Set;

public interface RedisMapper extends Serializable {

    RedisCommand getCommand();

    Integer getKeyTtl();

    String getAdditionalKey();

    RedisFormatter<?> getFormatter();

    String getPrimaryKey();

    String[] getFieldNames();

    TypeInformation<?>[] getFieldTypes();

    Set<RedisCommand> getOptionalCommand();

}
