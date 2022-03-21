package org.luna.learn.flink.udfs;

import org.apache.flink.connector.redis.RedisUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author Liu Yang
 * @date 2022/3/15 8:52
 */
@FunctionHint(
        input = {@DataTypeHint("STRING")},
        output = @DataTypeHint("STRING")
)
@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING")},
        output = @DataTypeHint("STRING")
)
public class GetRedisDimensionCode extends ScalarFunction {

    public @DataTypeHint("STRING") String eval(String key) {
        return RedisUtils.get(key);
    }

    public @DataTypeHint("STRING") String eval(String key, String field) {
        return RedisUtils.hget(key, field);
    }
}
