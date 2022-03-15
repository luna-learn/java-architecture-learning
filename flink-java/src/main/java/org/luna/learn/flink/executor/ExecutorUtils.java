package org.luna.learn.flink.executor;

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
        LocalExecutor executor = LocalExecutor.builder()
                .config(config)
                .enableWebUI(enableWebUI)
                .build();
        try {
            executor.start();
            return executor;
        } catch (Exception e) {
            throw new RuntimeException("Start executor fail.", e);
        }

    }
}
