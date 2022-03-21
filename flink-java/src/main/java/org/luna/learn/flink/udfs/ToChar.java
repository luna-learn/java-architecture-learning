package org.luna.learn.flink.udfs;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author Liu Yang
 * @date 2022/3/8 14:15
 */
@FunctionHint(input = {@DataTypeHint("BIGINT")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("BOOLEAN")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("DATE")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("DECIMAL")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("DOUBLE")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("FLOAT")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("INT")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("LONG")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("SMALLINT")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("STRING")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("TIME")}, output = @DataTypeHint("STRING"))
@FunctionHint(input = {@DataTypeHint("TIMESTAMP")}, output = @DataTypeHint("STRING"))
public class ToChar extends ScalarFunction {

    public @DataTypeHint("STRING") String eval(Object o) {
        return String.valueOf(o);
    }
}
