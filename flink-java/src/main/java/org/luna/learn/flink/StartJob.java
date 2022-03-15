/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.luna.learn.flink;

import org.luna.learn.flink.executor.ConfigUtils;
import org.luna.learn.flink.executor.Executor;
import org.luna.learn.flink.executor.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StartJob {
	private static final Logger LOG = LoggerFactory.getLogger(StartJob.class);

	public static void main(String[] args) throws Exception {
		// 读入配置文件，启动执行器执行SQL或SQL文件
		// 检查是否传入配置文件
		String configFile = args.length == 0 ? "conf/application.properties" : args[0];

		// 读取配置文件
		Map<String, String> config = ConfigUtils.load(configFile);

		// 读取任务集合并开始执和
		ConfigUtils.getJobs(config).forEach(job -> {
			LOG.info("Start job: " + job);
			// 检测和应用任务执行模式和配置
			Map<String, String> runConf = ConfigUtils.getJobRunConf(config, job, false);
			String runMode = runConf.getOrDefault("mode", "standalone");
			LOG.info("Job [" + job + "] run mode: " + runMode);
			Executor executor = null;
			if ("chain".equals(runMode)) {
				LOG.info("Job [" + job + "] start a executor with " + runMode + "mode");
				executor = ExecutorUtils.getLocalExecutor(config);
			}
			// 开始执行SQL语句
			Map<Integer, String> sqlStatements = ConfigUtils.getJobSqlStatements(config, job, false);
			for (Integer key: sqlStatements.keySet()) {
				if ("standalone".equals(runMode)) {
					LOG.info("Job [" + job + "] start a executor with " + runMode + "mode");
					executor = ExecutorUtils.getLocalExecutor(config);
				}
				if (executor != null) {
					executor.executeSql(sqlStatements.get(key));
				}
			}
			// 开始执行SQL文件
			Map<Integer, String> sqlFiles = ConfigUtils.getJobSqlFiles(config, job, false);
			for (Integer key: sqlFiles.keySet()) {
				if ("standalone".equals(runMode)) {
					LOG.info("Job [" + job + "] start a executor with " + runMode + "mode");
					executor = ExecutorUtils.getLocalExecutor(config);
				}
				if (executor != null) {
					executor.executeSqlFile(sqlFiles.get(key));
				}
			}
		});
	}
}
