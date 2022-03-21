package org.luna.learn.flink.udfs;

import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * @author Liu Yang
 * @date 2022/3/2 8:44
 */
@FunctionHint(
        input = {@DataTypeHint("DATE"), @DataTypeHint("STRING")},
        output = @DataTypeHint("STRING")
)
@FunctionHint(
        input = {@DataTypeHint("TIME"), @DataTypeHint("STRING")},
        output = @DataTypeHint("STRING")
)
@FunctionHint(
        input = {@DataTypeHint("TIMESTAMP"), @DataTypeHint("STRING")},
        output = @DataTypeHint("STRING")
)
public class DateFormat extends ScalarFunction {
    private static final Cache<String, DateTimeFormatter> FORMATTER_CACHE = CacheBuilder.newBuilder()
            .maximumSize(10)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    public @DataTypeHint("STRING") String eval(LocalDate date, String format) {
        if (date == null) {
            return null;
        }
        DateTimeFormatter formatter = FORMATTER_CACHE.getIfPresent(format);
        if (formatter == null) {
            formatter = DateTimeFormatter.ofPattern(format);
            FORMATTER_CACHE.put(format, formatter);
        }
        return date.format(formatter);
    }

    public @DataTypeHint("STRING") String eval(LocalDateTime dateTime, String format) {
        if (dateTime == null) {
            return null;
        }
        DateTimeFormatter formatter = FORMATTER_CACHE.getIfPresent(format);
        if (formatter == null) {
            formatter = DateTimeFormatter.ofPattern(format);
            FORMATTER_CACHE.put(format, formatter);
        }
        return dateTime.format(formatter);
    }
}
