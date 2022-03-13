package org.luna.learn.flink.connector.redis.table.source;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.luna.learn.flink.connector.redis.config.RedisOptions;
import org.luna.learn.flink.connector.redis.mapper.RedisMapper;
import org.luna.learn.flink.connector.redis.mapper.RedisQueryMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStreamTableSourceFactory implements StreamTableSourceFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> required = new HashMap<>();

        return required;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> supported = new ArrayList<>();

        return supported;
    }

    @Override
    public TableSource<Row> createTableSource(Map<String, String> properties) {
        // TableSchema schema = context.getCatalogTable().getSchema();
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        // descriptorProperties.getTableSchema("schema");
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema("schema"));
        RedisMapper mapper = RedisQueryMapper.builder()
                .setFieldNames(schema.getFieldNames())
                .setFieldTypes(schema.getFieldTypes())
                .build();
        return new RedisLookupableTableSource(RedisOptions.getConnectorOptions(properties),
                RedisOptions.getSourceOptions(properties),
                mapper);
    }
}
