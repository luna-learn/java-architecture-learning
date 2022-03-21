package org.luna.learn.flink.udfs;

import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.ScalarFunction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @author Liu Yang
 * @date 2022/3/2 8:44
 */
@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING")},
        output = @DataTypeHint("TIMESTAMP")
)
@FunctionHint(
        input = {@DataTypeHint("CHARACTER"), @DataTypeHint("CHARACTER")},
        output = @DataTypeHint("TIMESTAMP")
)
public class ToTimestamp extends ScalarFunction {
    private static final Cache<String, LocalDateTime> CACHE = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();
    private static final Cache<String, Date> DATE_CACHE = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();
    private static final Cache<String, DateFormat> DATE_FORMAT_CACHE = CacheBuilder.newBuilder()
            .maximumSize(10)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();
    private static final Cache<String, DateTimeFormatter> FORMATTER_CACHE = CacheBuilder.newBuilder()
            .maximumSize(10)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    public @DataTypeHint("TIMESTAMP(6)") LocalDateTime eval(String str, String format) {
        if (str == null || format == null) {
            return null;
        }
        LocalDateTime result = CACHE.getIfPresent(str);
        if (result == null) {
            DateTimeFormatter formatter = FORMATTER_CACHE.getIfPresent(format);
            if (formatter == null) {
                formatter = DateTimeFormatter.ofPattern(format);
                FORMATTER_CACHE.put(format, formatter);
            }
            TemporalAccessor accessor = formatter.parse(str);
            LocalDate date = LocalDate.from(accessor);
            LocalTime time = LocalTime.from(accessor);
            result = LocalDateTime.of(date, time);
            CACHE.put(str, result);
        }
        return result;
    }

    public @DataTypeHint("TIMESTAMP(6)") LocalDateTime eval0(String str, String format) {
        String[] arr1 = str.split("\\.");
        String[] arr2 = format.split("\\.");
        if (arr1.length != arr2.length || arr1.length > 3) {
            throw new DateTimeException("input value does not match format");
        }
        if (arr1.length > 2) {
            throw new DateTimeException("unsupported format and value");
        }
        if (arr1[0].length() != arr2[0].length()) {
            throw new DateTimeException("input value does not match format at datetime part: " +
                    arr1[0] + ", " + arr2[0]);
        }
        if (arr1.length == 2 && arr1[1].length() != arr2[1].length()) {
            throw new DateTimeException("input value does not match format at nano millisecond part: " +
                    arr1[1] + ", " + arr2[1]);
        }

        try {
            LocalDateTime result = CACHE.getIfPresent(str);
            if (result != null) {
                return result;
            }
            Date date = DATE_CACHE.getIfPresent(arr1[0]);
            if (date == null) {
                DateFormat formatter = DATE_FORMAT_CACHE.getIfPresent(format);
                if (formatter == null) {
                    formatter = new SimpleDateFormat(arr2[0]);
                    DATE_FORMAT_CACHE.put(format, formatter);
                }
                date = formatter.parse(arr1[0]);
                DATE_CACHE.put(arr1[0], date);
            }

            long mills = date.getTime();
            int nano = 0;
            if (arr1.length == 2) {
                if (!Pattern.matches("[S]{3,9}", arr2[1])) {
                    throw new DateTimeException("nano millisecond format syntax error => " + arr2[1]);
                }
                for(int i=arr1[1].length(); i < 9; i++) {
                    arr1[1] += "0";
                }
                mills += Integer.parseInt(arr1[1].substring(0,3));
                nano += Integer.parseInt(arr1[1].substring(3));
            }
            result = TimestampData.fromEpochMillis(mills, nano).toLocalDateTime();
            CACHE.put(str, result);
            return result;
        } catch (ParseException e) {
            throw new DateTimeException("parse timestamp fail", e);
        }
    }
}
