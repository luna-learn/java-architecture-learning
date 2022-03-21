package org.luna.learn.flink.udfs;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * @author Liu Yang
 * @date 2022/3/9 14:18
 */
@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING")},
        output = @DataTypeHint("DATE")
)

@FunctionHint(
        input = {@DataTypeHint("TIMESTAMP")},
        output = @DataTypeHint("DATE")
)
public class ToDate extends ScalarFunction {
    public @DataTypeHint("DATE") LocalDate eval(String str, String format) {
        if (str == null || format == null) {
            return null;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        TemporalAccessor accessor = formatter.parse(str);
        return LocalDate.from(accessor);
    }
    public @DataTypeHint("DATE") LocalDate eval(LocalDateTime timestamp) {
        if (timestamp == null) {
            return null;
        }
        return timestamp.toLocalDate();
    }
}
