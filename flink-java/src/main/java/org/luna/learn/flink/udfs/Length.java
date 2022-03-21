package org.luna.learn.flink.udfs;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author Liu Yang
 * @date 2022/3/17 15:22
 */
@FunctionHint(
        input = {@DataTypeHint("STRING")},
        output = @DataTypeHint("INT")
)
public class Length extends ScalarFunction {
    public @DataTypeHint("INT") Integer eval(String s) {
        if (s == null) {
            return null;
        }
        return s.length();
    }

}
