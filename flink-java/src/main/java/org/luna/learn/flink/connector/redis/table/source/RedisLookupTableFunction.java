package org.luna.learn.flink.connector.redis.table.source;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.luna.learn.flink.connector.redis.config.RedisConnectorOptions;
import org.luna.learn.flink.connector.redis.config.RedisSourceOptions;
import org.luna.learn.flink.connector.redis.container.RedisContainer;
import org.luna.learn.flink.connector.redis.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisLookupTableFunction extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupTableFunction.class);

    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisMapper redisMapper;
    private RedisContainer redisContainer;

    public RedisLookupTableFunction(RedisConnectorOptions connectorOptions,
                                    RedisSourceOptions sourceOptions,
                                    RedisMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;
    }

    public void open(FunctionContext context) {
        redisContainer = connectorOptions.getContainer();

    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(redisMapper.getFieldTypes(), redisMapper.getFieldNames());
    }

    public void eval(RowData row){
        System.out.println(row);
        LOG.info("{}", row);
        // collect(s);
        // GenericRowData.ofKind(RowKind.INSERT, "test", "test")
        collect(Row.ofKind(RowKind.INSERT, "test"));
    }

    public void close() throws Exception {
        if (redisContainer != null) {
            redisContainer.close();
        }
    }
}
