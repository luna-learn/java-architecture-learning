package org.luna.learn.flink.udfs;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @author Liu Yang
 * @date 2022/3/9 10:04
 */
@FunctionHint(
        input = {@DataTypeHint("DATE")},
        output = @DataTypeHint("LONG")
)
@FunctionHint(
        input = {@DataTypeHint("TIMESTAMP")},
        output = @DataTypeHint("LONG")
)
public class Days extends ScalarFunction {
    public @DataTypeHint("LONG") Long eval(LocalDate date) {
        if (date == null) {
            return null;
        }
        return date.toEpochDay();
    }

    public @DataTypeHint("LONG") Long eval(LocalDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.toLocalDate().toEpochDay();
    }
}
