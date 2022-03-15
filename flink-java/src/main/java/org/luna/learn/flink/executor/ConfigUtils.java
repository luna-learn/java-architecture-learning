package org.luna.learn.flink.executor;


import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ExecuteConfigUtils
 * @author Liu Yang
 * @date 2022/3/15 13:09
 */
public final class ConfigUtils {

    public static Map<String, String> load(String path) {
        try {
            ParameterTool prop = ParameterTool.fromPropertiesFile(path);
            return prop.toMap();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Can not load config from " + path);
        }
    }

    public static Set<String> getJobs(Map<String, String> properties) {
        return Arrays.stream(properties.getOrDefault("flink.job.execute", "")
                .split(","))
                .collect(Collectors.toSet());
    }

    public static Map<Integer, String> getJobSqlStatements(Map<String, String> properties, String jobName, boolean withPrefix) {
        String prefix = "flink.job." + jobName + ".sql.";
        return properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(k -> Integer.parseInt(k.getKey().replace(prefix, "")),
                        Map.Entry::getValue));
    }

    public static Map<Integer, String> getJobSqlFiles(Map<String, String> properties, String jobName, boolean withPrefix) {
        String prefix = "flink.job." + jobName + ".sql-file.";
        return properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(k -> Integer.parseInt(k.getKey().replace(prefix, "")),
                        Map.Entry::getValue));
    }

    public static Map<String, String> getJobCacheConf(Map<String, String> properties, String jobName, boolean withPrefix) {
        String prefix = "flink.job." + jobName + ".cache.";
        return properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(e -> withPrefix ? e.getKey().replace(prefix, ""): e.getKey(),
                        Map.Entry::getValue));
    }

    public static Map<String, String> getJobRunConf(Map<String, String> properties, String jobName, boolean withPrefix) {
        String prefix = "flink.job." + jobName + ".cache.";
        return properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(e -> withPrefix ? e.getKey().replace(prefix, ""): e.getKey(),
                        Map.Entry::getValue));
    }

}
