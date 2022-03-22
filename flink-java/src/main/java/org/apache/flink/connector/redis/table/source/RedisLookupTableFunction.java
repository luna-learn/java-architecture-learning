package org.apache.flink.connector.redis.table.source;


import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisFormatter;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RedisLookupTableFunction extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupTableFunction.class);

    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisMapper redisMapper;
    private RedisContainer redisContainer;
    private final RedisFormatter formatter;
    private final String[] fieldNames;
    private final DataType[] dataTypes;
    private final LogicalType[] logicalTypes;

    private final int fieldNum;
    private final String primaryKey;
    private int primaryKeyIndex = 0;
    private String cursor = "0";
    private ScanResult<Map.Entry<String, String>> scanResult;
    private ScanParams scanParams;
    private List<Map.Entry<String, String>> scanBuffer;

    private final int cacheExpireMs;
    private final int cacheMaxSize;
    private final int maxRetryTimes;
    private final String additionalKey;
    private Cache<String, GenericRowData> cache;

    public RedisLookupTableFunction(RedisConnectorOptions connectorOptions,
                                    RedisSourceOptions sourceOptions,
                                    RedisMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;

        this.fieldNames = redisMapper.getFieldNames();
        this.dataTypes = redisMapper.getDataTypes();
        this.additionalKey = redisMapper.getAdditionalKey();
        this.cacheExpireMs = sourceOptions.getCacheExpireMs();
        this.cacheMaxSize = sourceOptions.getCacheMaxSize();
        this.maxRetryTimes =  sourceOptions.getMaxRetryTimes();

        this.primaryKey = redisMapper.getPrimaryKey();
        this.fieldNum = fieldNames.length;
        for (int i=0; i<fieldNum; i++) {
            if (Objects.equals(primaryKey, fieldNames[i])) {
                this.primaryKeyIndex = i;
                break;
            }
        }

        this.logicalTypes = new LogicalType[fieldNum];
        for (int i=0; i<fieldNum; i++) {
            this.logicalTypes[i] = dataTypes[i].getLogicalType();
        }
        this.formatter = new RedisFormatter();
    }

    public void open(FunctionContext context) throws Exception {
        redisContainer = connectorOptions.getContainer();
        redisContainer.open();
        //
        if (cache == null && cacheExpireMs > 0 && cacheMaxSize > 0) {
            cache = CacheBuilder.newBuilder()
                    .recordStats()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
        }
    }

    public void eval(Object... values){
        String key = String.valueOf(values[0]);
        GenericRowData row;
        if (cache != null) {
            row = cache.getIfPresent(key);
            if (row != null) {
                collect(row);
                return;
            }
        }

        for (int i=0; i<maxRetryTimes; i++) {
            try {
                row = new GenericRowData(fieldNum);
                for(int j=0; j<fieldNum; j++) {
                    String value = redisContainer.hget(additionalKey + ":" + fieldNames[j],
                            key);
                    row.setField(j, formatter.encode(value, logicalTypes[j]));
                }
                if (cache != null) {
                    cache.put(key, row);
                }
                collect(row);
                break;
            } catch (Exception e) {
                if (i >= this.maxRetryTimes - 1) {
                    throw new RuntimeException("Execution of redis lookup failed.", e);
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

        }


    }

    public void close() throws Exception {
        if (redisContainer != null) {
            redisContainer.close();
        }
    }
}
