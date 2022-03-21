package org.luna.learn.flink.executor;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Liu Yang
 * @date 2022/3/15 9:12
 */
public final class LocalExecutor implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);
    private static final String UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG = "Unsupported SQL query! executeSql() only accepts a single SQL statement of type " +
            "CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE DATABASE, DROP DATABASE, ALTER DATABASE, " +
            "CREATE FUNCTION, DROP FUNCTION, ALTER FUNCTION, CREATE CATALOG, DROP CATALOG, " +
            "USE CATALOG, USE [CATALOG.]DATABASE, SHOW CATALOGS, SHOW DATABASES, SHOW TABLES, SHOW [USER] FUNCTIONS, SHOW PARTITIONS" +
            "CREATE VIEW, DROP VIEW, SHOW VIEWS, INSERT, DESCRIBE, LOAD MODULE, UNLOAD " +
            "MODULE, USE MODULES, SHOW [FULL] MODULES.";

    private final Configuration envConfig;
    private final EnvironmentSettings tableEnvSetting;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;
    private Map<String, String> config;

    private List<String> jobIds;

    private LocalExecutor() {
        envConfig = new Configuration();
        tableEnvSetting = EnvironmentSettings.newInstance()
                .inStreamingMode().build();
    }

    @Override
    public void start() throws Exception {
        envConfig.setString("web.log.path",
                config.getOrDefault("web.log.path", "logs/flink.log"));
        envConfig.setString(RestOptions.BIND_PORT,
                config.getOrDefault("rest.bind-port", "8081-8089"));
        envConfig.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,
                config.getOrDefault("taskmanager.log.path", "logs/flink.log"));
        // 启动 flink
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(envConfig);
        tableEnv = StreamTableEnvironment.create(env, tableEnvSetting);
        // 本地模式设置并行度
        env.setParallelism(Integer.parseInt(
                config.getOrDefault("flink.env.parallelism", "1")));
        // 设置状态和检查点储存策略
        env.setStateBackend(new FsStateBackend(
                config.getOrDefault("flink.env.state.backend.fs.path", "file:///opt/checkpoint/sih")));
        // 开启检查点
        env.enableCheckpointing(Integer.parseInt(
                config.getOrDefault("flink.env.checkpoint.interval", "5000")));
        // 设置检查点模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间。单位毫秒
        env.getCheckpointConfig().setCheckpointTimeout(Integer.parseInt(
                config.getOrDefault("flink.env.checkpoint.timeout", "600000")));
        // 设置最大并发执行的检查点的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(Integer.parseInt(
                config.getOrDefault("flink.env.checkpoint.concurrent.max", "1")));
        // 设置检查点失败次数重启作业门限
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.parseInt(
                config.getOrDefault("flink.env.checkpoint.tolerable.failure.number", "3")));
        // 将检查点持久化到外部存储
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 设置时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tableEnv.getConfig().getConfiguration().setFloat("taskmanager.memory.network.fraction", 0.15f);
        tableEnv.getConfig().getConfiguration().setString("taskmanager.memory.network.min", "128mb");
        tableEnv.getConfig().getConfiguration().setString("taskmanager.memory.network.max", "4gb");
        tableEnv.getConfig().getConfiguration().setInteger("taskmanager.network.numberOfBuffers", 16);

        //tableEnv.registerFunction("to_char", new ToChar());
        //tableEnv.registerFunction("to_date", new ToDate());
        //tableEnv.registerFunction("to_timestamp", new ToTimestamp());
        //tableEnv.registerFunction("date_format", new DateFormat());
        //tableEnv.registerFunction("days", new Days());
        //tableEnv.registerFunction("length", new Length());
    }

    @Override
    public void stop() throws Exception {
        if (jobIds != null && jobIds.size() > 0) {
            List<CustomCommandLine> commandLines = new ArrayList<>();
            commandLines.add(new DefaultCLI());
            CliFrontend cli = new CliFrontend(envConfig, commandLines);
            jobIds.forEach(e -> {
                cli.parseAndRun(new String[] {"cannel", e});
            });
        }

    }

    @Override
    public void executeSql(String sql, boolean printResult, boolean printSchema) {
        if (sql == null || sql.isEmpty() || sql.trim().isEmpty()) {
            return;
        }
        String executableSql = sql;
        try {
            executableSql = executableSql.trim();
            if (sql.endsWith(";")) {
                executableSql = executableSql.substring(0, sql.length() - 1);
            }
            // 替换变量
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            // 复制配置，避免修改配置造成关联影响
            Map<String, String> configCopy = config.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // 修改配置
            configCopy.put("currentDate", LocalDate.now().format(formatter));
            // 应用配置
            for (Map.Entry<String, String> entry: configCopy.entrySet()) {
                executableSql = executableSql.replace("${" + entry.getKey() + "}", entry.getValue());
            }
            // 执行 SQL
            LOG.info(">>> executing: " + executableSql);
            // 解析SQL
            List<Operation> operations = ((TableEnvironmentImpl) tableEnv).getParser().parse(executableSql);
            if (operations.size() != 1) {
                throw new ValidationException(UNSUPPORTED_QUERY_IN_EXECUTE_SQL_MSG);
            }
            Operation operation = operations.get(0);
            if (operation instanceof QueryOperation
                    && !(operation instanceof ModifyOperation)) {
                Table table = tableEnv.sqlQuery(executableSql);
                if (executableSql.contains("@export csv")) {
                    int a = executableSql.indexOf("@export csv") + "@export csv".length();
                    int b = executableSql.indexOf('\n', a + 1);
                    String path = executableSql.substring(a, b).trim();
                    exportCSV(table, path);
                }
                if (printResult || executableSql.contains("@print result")) {
                    table.limit(20).execute().print();
                }
                if (printSchema || executableSql.contains("@print schema")) {
                    table.printSchema();
                }
            } else {
                TableResult result = tableEnv.executeSql(executableSql);
                showJobState(result);
            }
        } catch (Exception e) {
            // e.printStackTrace();
            throw new RuntimeException("Execute sql error: " + sql, e);
        }
    }

    @Override
    public void executeSqlFile(String sqlFile) {
        if (sqlFile == null || sqlFile.trim().isEmpty()) {
            return;
        }
        try {
            LOG.info("Parse sql file " + sqlFile);
            CloseableIterator<String> sqlStatements = env.readTextFile(sqlFile)
                    .executeAndCollect("Parse sql file " + sqlFile);
            final StringBuffer sqlContent = new StringBuffer();
            sqlStatements.forEachRemaining(e -> sqlContent.append(e).append("\n"));
            for (String sql: sqlContent.toString().split(";\r\n|;\n")) {
                if (!sql.isEmpty()) {
                    executeSql(sql);
                }
            }
            // env.execute("Execute sql file " + sqlFile);
        } catch (Exception e) {
            // e.printStackTrace();
            throw new RuntimeException("Execute sql file error: " + sqlFile, e);
        }
    }

    @Override
    public void registerFunction(String name, TableFunction function) {
        tableEnv.registerFunction(name, function);
    }

    @Override
    public void registerFunction(String name, TableAggregateFunction function) {
        tableEnv.registerFunction(name, function);
    }

    @Override
    public void registerFunction(String name, ScalarFunction function) {
        tableEnv.registerFunction(name, function);
    }

    @Override
    public void registerFunction(String name, AggregateFunction function) {
        tableEnv.registerFunction(name, function);
    }

    private void showJobState(TableResult tableResult) {
        if (tableResult.getJobClient().isPresent()) {
            JobClient job = tableResult.getJobClient().get();
            LOG.info(">>> jobId: " + job.getJobID().toString());
        }
    }

    private void exportCSV(Table table, String csvFile) throws Exception {
        if (table == null || csvFile == null || csvFile.isEmpty()) {
            return;
        }
        LOG.info(">>> export csv: " + csvFile);
        TableSchema tableSchema = table.getSchema();
        final String[] fieldNames = tableSchema.getFieldNames();
        final DataType[] dataTypes = tableSchema.getFieldDataTypes();
        // 累加器
        final IntCounter counter = new IntCounter();
        // 构建文本输出
        TextOutputFormat<String> out = new TextOutputFormat<>(new Path(csvFile), "UTF-8");
        out.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        out.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
        out.open(1, 1); // 设置任务数和并发数
        // 消费数据
        Consumer<Tuple2<Boolean, Row>> consumer = tuple -> {
            try {
                if (counter.getLocalValuePrimitive() == 0) {
                    StringBuilder header = new StringBuilder();
                    for(int i=0, l=fieldNames.length; i<l; i++) {
                        if (i > 0) {
                            header.append(',');
                        }
                        header.append('"').append(fieldNames[i]).append('"');
                    }
                    out.writeRecord(header.toString());
                }
                StringBuilder sb = new StringBuilder();
                Row row = tuple.f1;
                for(int i=0, l=fieldNames.length; i<l; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    switch (dataTypes[i].getLogicalType().getTypeRoot()) {
                        case FLOAT:
                        case DECIMAL:
                        case DOUBLE:
                        case INTEGER:
                        case BIGINT:
                        case SMALLINT:
                            sb.append(row.getField(i));
                        default:
                            sb.append('"').append(row.getField(i)).append('"');
                    }
                }
                if (sb.length() > 0) {
                    counter.add(1);
                    out.writeRecord(sb.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        // 将 Table 转换为 RetractStream 后写入 OutputFormat
        tableEnv.toRetractStream(table, Row.class)
                .executeAndCollect("export csv " + csvFile)
                .forEachRemaining(consumer);
        //env.execute("export csv " + csvFile);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final LocalExecutor executor;
        private Builder() {
            executor = new LocalExecutor();
            executor.config = new HashMap<>();
            executor.jobIds = new ArrayList<>();
        }

        public Builder enableWebUI(boolean enable) {
            executor.envConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            return this;
        }

        public Builder config(Map<String, String> config) {
            if (config != null) {
                executor.config = config;
            }
            return this;
        }

        public LocalExecutor build() {
            return executor;
        }
    }
}
