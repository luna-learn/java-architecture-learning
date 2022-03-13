package org.luna.learn.flink.connector.redis.table.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.luna.learn.flink.connector.redis.config.RedisConnectorOptions;
import org.luna.learn.flink.connector.redis.config.RedisSinkOptions;
import org.luna.learn.flink.connector.redis.container.RedisContainer;
import org.luna.learn.flink.connector.redis.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class RedisSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    private final RedisConnectorOptions connectorOptions;
    private final RedisSinkOptions sinkOptions;
    private final RedisMapper redisMapper;

    private final String[] fieldNames;
    private final TypeInformation<?>[] fieldTypes;
    private final String additionalKey;
    private final Integer ttl;

    private final int fieldNum;
    private final String primaryKey;
    private int primaryKeyIndex = 0;

    private RedisContainer redisContainer;


    public RedisSink(RedisConnectorOptions connectorOptions, RedisSinkOptions sinkOptions, RedisMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sinkOptions = sinkOptions;
        this.redisMapper = redisMapper;

        this.fieldNames = redisMapper.getFieldNames();
        this.fieldTypes = redisMapper.getFieldTypes();
        this.additionalKey = redisMapper.getAdditionalKey();
        this.ttl = redisMapper.getKeyTtl();

        this.primaryKey = redisMapper.getPrimaryKey();
        this.fieldNum = fieldNames.length;
        for (int i=0; i<fieldNum; i++) {
            if (Objects.equals(primaryKey, fieldNames[i])) {
                this.primaryKeyIndex = i;
                break;
            }
        }

    }

    @Override
    public void invoke(IN input, Context context) throws Exception {

        String key = null;
        String value = null;

        if (input instanceof RowData) {
            RowData row =  (RowData) input;
            int length = row.getArity();
            if (length != fieldNames.length) {
                throw new IllegalArgumentException("Input RowData length not equals fieldNames, " +
                        "required: " + fieldNames.length + ", " +
                        "provided: " + length);
            }
            key = String.valueOf(row.getString(primaryKeyIndex));
            for (int i=0; i<length; i++) {
                try {
                    value = String.valueOf(row.getString(i));
                    redisContainer.hset(additionalKey + ":" + fieldNames[i], key, value);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }

            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisContainer = connectorOptions.getContainer();
        redisContainer.open();
    }

    @Override
    public void close() throws IOException {
        if (redisContainer != null) {
            redisContainer.close();
        }
    }

}
