package org.luna.learn.flink.executor;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.descriptors.Schema;

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
    default void registerUDF() {}

}
