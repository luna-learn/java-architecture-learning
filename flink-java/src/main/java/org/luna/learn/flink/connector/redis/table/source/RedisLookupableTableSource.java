package org.luna.learn.flink.connector.redis.table.source;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.luna.learn.flink.connector.redis.config.RedisConnectorOptions;
import org.luna.learn.flink.connector.redis.config.RedisSourceOptions;
import org.luna.learn.flink.connector.redis.mapper.RedisMapper;

public class RedisLookupableTableSource implements LookupableTableSource<Row> {

    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisMapper redisMapper;

    public RedisLookupableTableSource(RedisConnectorOptions connectorOptions,
                                      RedisSourceOptions sourceOptions,
                                      RedisMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] strings) {
        return new RedisLookupTableFunction(connectorOptions,
                sourceOptions,
                redisMapper);
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] strings) {
        return null;
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(redisMapper.getFieldNames(),
                        TypeConversions.fromLegacyInfoToDataType(redisMapper.getFieldTypes()))
                .build();
    }

    @Override
    public DataType getProducedDataType() {
        // 旧版本的Typeinfo类型转新版本的DataType
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(redisMapper.getFieldTypes(),
                redisMapper.getFieldNames()));
    }
}
