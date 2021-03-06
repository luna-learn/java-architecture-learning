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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		Configuration envConfig = new Configuration();
		envConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		envConfig.setString("web.log.path", "logs/flink.log");
		envConfig.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "logs/flink.log");
		envConfig.setString(RestOptions.BIND_PORT, "8081-8089");

		EnvironmentSettings tableEnvSetting = EnvironmentSettings
				.newInstance()
				.inStreamingMode().build();
		// set up the streaming execution environment
		StreamExecutionEnvironment env  = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(envConfig);
		StreamTableEnvironment tableEnv  = StreamTableEnvironment.create(env, tableEnvSetting);
		env.setParallelism(1); // ??????????????????????????????1

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		tableEnv.executeSql("CREATE TABLE REDIS_SOURCE (" +
				" NAME VARCHAR(100), " +
				" `VALUE` VARCHAR(100)," +
				" PRIMARY KEY (NAME) NOT ENFORCED) \n" +
				"WITH (\n" +
				"  'connector' = 'redis',\n" +
				"  'mode' = 'single',\n" +
				"  'host' = 'localhost',\n" +
				"  'port' = '6379',\n" +
				"  'additional-key' = 'sap:test:*', \n" +
				"  'lookup.cache.max-size' = '1000', \n" +
				"  'lookup.cache.expire-ms' = '60000' \n" +
				")");
		tableEnv.executeSql("CREATE TABLE ZDMS24 (\n" +
				"                            MANDT VARCHAR(9),\n" +
				"                            OBJECTCLASS VARCHAR(54),\n" +
				"                            VHCLE VARCHAR(30),\n" +
				"                            ZSPD VARCHAR(30),\n" +
				"                            ZSPT VARCHAR(30),\n" +
				"                            ZSPZ VARCHAR(12),\n" +
				"                            ZSPB VARCHAR(765),\n" +
				"                            ZSPP VARCHAR(30),\n" +
				"                            ZSRP VARCHAR(30),\n" +
				"                            AUD_TYPE VARCHAR(30),\n" +
				"                            AUD_TIME TIMESTAMP(3),\n" +
				"                            PRIMARY KEY (VHCLE, AUD_TYPE) NOT ENFORCED,\n" +
				"                            WATERMARK FOR AUD_TIME AS AUD_TIME - INTERVAL '6' HOUR\n" +
				") WITH (\n" +
				"  'connector.type' = 'jdbc',\n" +
				"  'connector.driver' = 'com.mysql.cj.jdbc.Driver',\n" +
				"  'connector.url' = 'jdbc:mysql://localhost:3306/manager',\n" +
				"  'connector.table' = 'ZDMS24',\n" +
				"  'connector.username' = 'root',\n" +
				"  'connector.password' = 'root'\n" +
				" )");
		tableEnv.executeSql("CREATE TABLE MYSQL_SOURCE (\n" +
				"NAME VARCHAR(100), \n" +
				"`VALUE` VARCHAR(100), \n" +
				"UPDATE_TIME TIMESTAMP(3), \n" +
				" PRIMARY KEY (NAME) NOT ENFORCED\n" +
				"-- WATERMARK FOR UPDATE_TIME AS UPDATE_TIME - INTERVAL '1' DAY \n" +
				") \n" +
				"WITH (\n" +
				"  'connector.type' = 'jdbc',\n" +
				"  'connector.driver' = 'com.mysql.cj.jdbc.Driver',\n" +
				"  'connector.url' = 'jdbc:mysql://localhost:3306/manager',\n" +
				"  'connector.username' = 'root',\n" +
				"  'connector.password' = 'root',\n" +
				"  'connector.table' = 'test_streaming'\n" +
				")");
		tableEnv.executeSql("SELECT NAME, `VALUE` FROM MYSQL_SOURCE")
				.print();

		tableEnv.executeSql("WITH TAB_A AS (SELECT T.*,\n" +
						"  LAG(VHCLE) OVER(PARTITION BY `MANDT` ORDER BY AUD_TIME ) AS L_VHCLE\n" +
						" FROM ZDMS24 T) SELECT * FROM TAB_A")
				.print();

		tableEnv.executeSql("SELECT NAME, `VALUE` FROM REDIS_SOURCE")
				.print();

		tableEnv.executeSql("SELECT A.NAME, A.`VALUE`, B.`VALUE` AS VALUE2\n" +
						" FROM MYSQL_SOURCE A \n" +
						"LEFT JOIN REDIS_SOURCE B ON A.NAME=B.NAME")
				.print();



		// execute program
		// env.execute("Flink Streaming Java API Skeleton");
	}
}
