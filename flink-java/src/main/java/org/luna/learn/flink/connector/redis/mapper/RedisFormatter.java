package org.luna.learn.flink.connector.redis.mapper;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class RedisFormatter implements Serializable {

    public Object decode(RowData row, int index, LogicalType logicalType) {
        if (row.isNullAt(index)) return null;
        switch(logicalType.getTypeRoot()) {
            case VARCHAR:
                return String.valueOf(row.getString(index));
            case DECIMAL:
                DecimalType decimalType = (DecimalType)   logicalType;
                return row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                return row.getTimestamp(index, timestampType.getPrecision());
            case DATE:
                int days = row.getInt(index);
                return LocalDate.ofEpochDay(days);
            case INTEGER:
                return row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            case SMALLINT:
                return row.getShort(index);
            case FLOAT:
                return row.getFloat(index);
            case DOUBLE:
                return row.getDouble(index);
            default:
                return null;
        }
    }

    public Object encode(String value, LogicalType logicalType) {
        if (value == null || "null".equals(value)) {
            return null;
        }
        switch(logicalType.getTypeRoot()) {
            case VARCHAR:
                return StringData.fromString(value);
            case DECIMAL:
                DecimalType decimalType = (DecimalType)   logicalType;
                return DecimalData.fromBigDecimal(new BigDecimal(value), decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromLocalDateTime(LocalDateTime.parse(value));
            case DATE:
                return LocalDate.parse(value);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case SMALLINT:
                return Short.parseShort(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            default:
                return null;
        }
    }

    public RedisFormatter() {

    }
}
