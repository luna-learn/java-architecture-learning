package org.luna.learn.flink.executor;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.Map;

/**
 * @author Liu Yang
 * @date 2022/3/15 14:58
 */
public class ExecutorUtils {
    public static Executor getExecutor(Map<String, String> config) {

        return null;
    }

    public static Executor getLocalExecutor(Map<String, String> config) {
        boolean enableWebUI = Boolean.parseBoolean(config.getOrDefault("flink.env.web.ui.enable", "false"));

        final LocalExecutor executor = LocalExecutor.builder()
                .config(config)
                .enableWebUI(enableWebUI)
                .build();
        try {
            executor.start();
            // 注册函数
            ConfigUtils.getUserDefinedFunctions(config).forEach((key, value) -> {
                try {
                    Object function = value.newInstance();
                    if (function instanceof TableFunction) {
                        executor.registerFunction(key, (TableFunction) function);
                    } else if (function instanceof TableAggregateFunction) {
                        executor.registerFunction(key, (TableAggregateFunction) function);
                    } else if (function instanceof ScalarFunction) {
                        executor.registerFunction(key, (ScalarFunction) function);
                    } else if (function instanceof AggregateFunction) {
                        executor.registerFunction(key, (AggregateFunction) function);
                    }
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
            return executor;
        } catch (Exception e) {
            throw new RuntimeException("Start executor fail.", e);
        }

    }
}
