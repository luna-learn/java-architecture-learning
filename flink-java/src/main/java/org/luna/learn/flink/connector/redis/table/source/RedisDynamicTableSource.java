package org.luna.learn.flink.connector.redis.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.luna.learn.flink.connector.redis.config.RedisConnectorOptions;
import org.luna.learn.flink.connector.redis.config.RedisSourceOptions;
import org.luna.learn.flink.connector.redis.mapper.RedisMapper;

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
        return TableFunctionProvider.of(new RedisLookupTableFunction(connectorOptions,
                sourceOptions,
                redisMapper));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {


        return InputFormatProvider.of(new RedisScanTableSource(connectorOptions,
                sourceOptions,
                redisMapper));
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
