package org.luna.learn.flink.executor;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author Liu Yang
 * @date 2022/3/15 9:09
 */
public interface Executor {

    // 启动
    void start() throws Exception;

    // 停止
    void stop() throws Exception;

    // 执行SQL语句
    void executeSql(String sql, boolean printResult, boolean printSchema);

    default void executeSql(String sql, boolean printResult) {
        executeSql(sql, printResult, false);
    }

    default void executeSql(String sql) {
        executeSql(sql, false, false);
    }

    // 执行SQL文件
    void executeSqlFile(String sqlFile);

    // 注册自定义函数
    void registerFunction(String name, TableFunction function);

    void registerFunction(String name, TableAggregateFunction function);

    void registerFunction(String name, ScalarFunction function);

    void registerFunction(String name, AggregateFunction function);

}
