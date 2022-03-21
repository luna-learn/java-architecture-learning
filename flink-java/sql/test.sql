CREATE TABLE MYSQL_SOURCE
(
    NAME VARCHAR(100),
    `VALUE` VARCHAR(100),
    UPDATE_TIME TIMESTAMP(3),
    PROCTIME AS PROCTIME(),
    PRIMARY KEY (NAME) NOT ENFORCED
    -- WATERMARK FOR UPDATE_TIME AS UPDATE_TIME - INTERVAL '1' DAY
) COMMENT ''
WITH (
    'connector.type' = 'jdbc',
    'connector.driver' = 'com.mysql.cj.jdbc.Driver',
    'connector.url' = 'jdbc:mysql://localhost:3306/manager',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.table' = 'test_streaming'
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

--@print result
SELECT
  A.NAME, A.`VALUE`, B.`VALUE` AS VALUE_B
FROM MYSQL_SOURCE A
LEFT JOIN REDIS_SINK FOR SYSTEM_TIME AS OF PROCTIME AS B ON A.NAME=B.NAME;