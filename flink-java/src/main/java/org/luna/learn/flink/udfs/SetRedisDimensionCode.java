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
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING")},
        output = @DataTypeHint("INT")
)
@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING"), @DataTypeHint("INT")},
        output = @DataTypeHint("INT")
)
@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING"), @DataTypeHint("STRING")},
        output = @DataTypeHint("INT")
)
@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING"), @DataTypeHint("STRING"), @DataTypeHint("INT")},
        output = @DataTypeHint("INT")
)
public class SetRedisDimensionCode extends ScalarFunction {

    public @DataTypeHint("INT") Integer eval(String key, String value, Integer ttl) {
        try {
            RedisUtils.set(key, value);
            if (ttl != null && ttl > 0) {

            }
            return 1;
        } catch (Exception e) {
            return 0;
        }
    }

    public @DataTypeHint("INT") Integer eval(String key, String value) {
        Integer ttl = null;
        return eval(key, value, ttl);
    }

    public @DataTypeHint("INT") Integer eval(String key, String field, String value, Integer ttl) {
        try {
            RedisUtils.hset(key, field, value);
            if (ttl != null && ttl > 0) {

            }
            return 1;
        } catch (Exception e) {
            return 0;
        }
    }

    public @DataTypeHint("INT") Integer eval(String key, String field, String value) {
        Integer ttl = null;
        return eval(key, field, value, ttl);
    }
}
