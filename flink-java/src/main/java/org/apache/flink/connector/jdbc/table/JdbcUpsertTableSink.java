//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class JdbcUpsertTableSink
        implements UpsertStreamTableSink<Row>, OverwritableTableSink, PartitionableTableSink {
    private final TableSchema schema;
    private final JdbcOptions options;
    private final int flushMaxSize;
    private final long flushIntervalMills;
    private final int maxRetryTime;
    private String[] keyFields;
    private boolean isAppendOnly;
    private boolean isOverwrite;

    private JdbcUpsertTableSink(TableSchema schema, JdbcOptions options, int flushMaxSize, long flushIntervalMills, int maxRetryTime) {
        this.schema = TableSchemaUtils.checkOnlyPhysicalColumns(schema);
        this.options = options;
        this.flushMaxSize = flushMaxSize;
        this.flushIntervalMills = flushIntervalMills;
        this.maxRetryTime = maxRetryTime;
    }

    private JdbcBatchingOutputFormat<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> newFormat() {
        if (this.isAppendOnly || this.keyFields != null && this.keyFields.length != 0) {
            int[] jdbcSqlTypes = Arrays.stream(this.schema.getFieldTypes()).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
            return JdbcBatchingOutputFormat.builder().setOptions(this.options).setFieldNames(this.schema.getFieldNames()).setFlushMaxSize(this.flushMaxSize).setFlushIntervalMills(this.flushIntervalMills).setMaxRetryTimes(this.maxRetryTime).setFieldTypes(jdbcSqlTypes).setKeyFields(this.keyFields).build();
        } else {
            throw new UnsupportedOperationException("JdbcUpsertTableSink can not support ");
        }
    }

    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        JdbcBatchingOutputFormat inputFormat = this.newFormat();
        if (isOverwrite) {
            // 执行 overwrite 逻辑
            try {
                Connection conn =  inputFormat.getConnection();
                if (conn == null) {
                    throw new SQLException("Can not get a connection from input format");
                }
                conn.createStatement()
                        .executeUpdate(options.getDialect().getTruncateTableStatement(options.getTableName()));
            } catch (SQLException e) {
                throw new UnsupportedOperationException("Jdbc connector execute overwrite exception.", e);
            }
        }
        return dataStream.addSink(new GenericJdbcSinkFunction(inputFormat))
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), this.schema.getFieldNames()));
    }

    public void setKeyFields(String[] keys) {
        this.keyFields = keys;
    }

    public void setIsAppendOnly(Boolean isAppendOnly) {
        this.isAppendOnly = isAppendOnly;
    }

    public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(new TypeInformation[]{Types.BOOLEAN, this.getRecordType()});
    }

    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(this.schema.getFieldTypes(), this.schema.getFieldNames());
    }

    public String[] getFieldNames() {
        return this.schema.getFieldNames();
    }

    public TypeInformation<?>[] getFieldTypes() {
        return this.schema.getFieldTypes();
    }

    public TableSchema getTableSchema() {
        return this.schema.copy();
    }

    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (Arrays.equals(this.getFieldNames(), fieldNames) && Arrays.equals(this.getFieldTypes(), fieldTypes)) {
            JdbcUpsertTableSink copy = new JdbcUpsertTableSink(this.schema, this.options, this.flushMaxSize, this.flushIntervalMills, this.maxRetryTime);
            copy.keyFields = this.keyFields;
            return copy;
        } else {
            throw new ValidationException("Reconfiguration with different fields is not allowed. Expected: " + Arrays.toString(this.getFieldNames()) + " / " + Arrays.toString(this.getFieldTypes()) + ". But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
    }

    @Override
    public void setOverwrite(boolean b) {
        isOverwrite = b;
    }

    @Override
    public void setStaticPartition(Map<String, String> partitions) {
        // 静态分区设置
    }

    @Override
    public boolean configurePartitionGrouping(boolean supportsGrouping) {
        return PartitionableTableSink.super.configurePartitionGrouping(supportsGrouping);
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean equals(Object o) {
        if (!(o instanceof JdbcUpsertTableSink)) {
            return false;
        } else {
            JdbcUpsertTableSink sink = (JdbcUpsertTableSink)o;
            return Objects.equals(this.schema, sink.schema) && Objects.equals(this.options, sink.options) && Objects.equals(this.flushMaxSize, sink.flushMaxSize) && Objects.equals(this.flushIntervalMills, sink.flushIntervalMills) && Objects.equals(this.maxRetryTime, sink.maxRetryTime) && Arrays.equals(this.keyFields, sink.keyFields) && Objects.equals(this.isAppendOnly, sink.isAppendOnly);
        }
    }

    public static class Builder {
        protected TableSchema schema;
        private JdbcOptions options;
        protected int flushMaxSize = 5000;
        protected long flushIntervalMills = 0L;
        protected int maxRetryTimes = 3;

        public Builder() {
        }

        public Builder setTableSchema(TableSchema schema) {
            this.schema = JdbcTypeUtil.normalizeTableSchema(schema);
            return this;
        }

        public Builder setOptions(JdbcOptions options) {
            this.options = options;
            return this;
        }

        public Builder setFlushMaxSize(int flushMaxSize) {
            this.flushMaxSize = flushMaxSize;
            return this;
        }

        public Builder setFlushIntervalMills(long flushIntervalMills) {
            this.flushIntervalMills = flushIntervalMills;
            return this;
        }

        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public JdbcUpsertTableSink build() {
            Preconditions.checkNotNull(this.schema, "No schema supplied.");
            Preconditions.checkNotNull(this.options, "No options supplied.");
            return new JdbcUpsertTableSink(this.schema, this.options, this.flushMaxSize, this.flushIntervalMills, this.maxRetryTimes);
        }
    }
}
