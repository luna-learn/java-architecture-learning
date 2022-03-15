CREATE TABLE MYSQL_SOURCE
(
    NAME VARCHAR(100),
    `VALUE` VARCHAR(100),
    UPDATE_TIME TIMESTAMP(3),
    PRIMARY KEY (NAME) NOT ENFORCED
    -- WATERMARK FOR UPDATE_TIME AS UPDATE_TIME - INTERVAL '1' DAY
) COMMENT ''
WITH (
    'connector.type' = 'jdbc',
    'connector.driver' = 'com.mysql.cj.jdbc.Driver',
    'connector.url' = 'jdbc:mysql://localhost:3306/sih',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.table' = 't_test_streaming'
);

CREATE TABLE REDIS_SINK
(
    NAME VARCHAR(100),
    `VALUE` VARCHAR(100),
    PRIMARY KEY (NAME) NOT ENFORCED
) COMMENT ''
WITH (
    'connector' = 'redis',
    'mode' = 'single',
    'host' = 'localhost',
    'port' = '6379',
    'additional-key' = 'sap:test1',
    'lookup.cache.max-size' = '1000',
    'lookup.cache.expire-ms' = '60000'
);

--@print result
SELECT NAME,`VALUE` FROM MYSQL_SOURCE;

--@print result
--@export csv test.csv
SELECT NAME,`VALUE` FROM REDIS_SINK;