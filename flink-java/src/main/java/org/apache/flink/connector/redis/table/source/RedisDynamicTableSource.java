package org.apache.flink.connector.redis.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

public class RedisDynamicTableSource implements LookupTableSource, ScanTableSource {


    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisMapper redisMapper;
    private final TableSchema schema;
    private final DataType physicalDataType;

    public RedisDynamicTableSource(RedisConnectorOptions connectorOptions,
                                   RedisSourceOptions sourceOptions,
                                   RedisMapper redisMapper,
                                   TableSchema schema) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;
        this.schema = schema;
        this.physicalDataType = schema.toPhysicalRowDataType();
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(connectorOptions,
                sourceOptions,
                redisMapper, schema);
    }

    @Override
    public String asSummaryString() {
        return "RedisDynamicTableSource";
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        // System.out.println(lookupContext.getKeys());
        String[] keyNames = new String[lookupContext.getKeys().length];

        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Redis only support non-nested look up keys");
            keyNames[i] = schema.getFieldNames()[innerKeyArr[0]];
        }
        // 检查给定的 keeName 是否为主键
        Preconditions.checkArgument(
                keyNames.length == 1 && keyNames[0].equals(redisMapper.getPrimaryKey()),
                "Redis only support look one primary key [" + redisMapper.getPrimaryKey() + "], but provided [" +
                String.join(",", keyNames) + "]");
        return TableFunctionProvider.of(new RedisLookupTableFunction(connectorOptions,
                sourceOptions,
                redisMapper));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // return ChangelogMode.all();
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (sourceOptions.isBounded()) {
            return InputFormatProvider.of(new RedisScanTableSource(connectorOptions,
                    sourceOptions,
                    redisMapper));
        } else {
            return SourceFunctionProvider.of(new RedisUnboundTableSource(connectorOptions,
                    sourceOptions,
                    redisMapper), sourceOptions.isBounded());
        }
    }

    private DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        } else {
            DataType physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection);
            if (prefix != null) {
                physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
            }

            return (DeserializationSchema)format.createRuntimeDecoder(context, physicalFormatDataType);
        }
    }
}
