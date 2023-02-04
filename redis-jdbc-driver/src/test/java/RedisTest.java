package com.itmuch.redis.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class RedisTest {
    private final static Logger LOGGER = new Logger(RedisTest.class);

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("com.itmuch.redis.jdbc.redis.RedisDriver");

        Connection connection = DriverManager.getConnection("jdbc:redis://localhost:6379/0");
        Statement statement = connection.createStatement();

        LOGGER.log("Initial schema is %s", connection.getSchema());
        statement.execute("USE    \"1\"   ");
        LOGGER.log("First schema is %s", connection.getSchema());
        statement.execute("USE  '11'");
        LOGGER.log("Test schema is %s", connection.getSchema());

        statement.execute("FLUSHDB");
        statement.execute("SET a b");
        ResultSet rs = statement.executeQuery("DBSIZE");
        rs.getMetaData().getColumnClassName(1);
        logResult(rs, "DB size");
        rs = statement.executeQuery("get a");
        logResult(rs, "Get result");

        rs = statement.executeQuery("TTL a");
        logResult(rs, "TTL result");

//        statement.execute("set a b");
//        ResultSet rs = statement.executeQuery("get a");
//        while (rs.next()) {
//            LOGGER.log("rs1:" + rs.getString(0));
//        }
//
        ResultSet resultSet = statement.executeQuery("keys *");
        logResult(resultSet, "Keys result");

        connection.setSchema("11");
        ResultSet resultSet2 = statement.executeQuery("set ab99 ab88");
        logResult(resultSet, "set result");

        statement.execute("HMSET hash field1 value1 field2 value2");
        rs = statement.executeQuery("HGET hash field1");
        logResult(rs, "HGET result");
        rs = statement.executeQuery("HGETALL hash");
        logResult(rs, "HGETALL result");


        resultSet.close();
        statement.close();
        connection.close();

//        statement.execute("ZADD runoobkey 2 mongodb");
//        statement.execute("ZADD runoobkey 3 elasticsearch");
//        statement.execute("ZADD runoobkey 4 mysql");
//
//        ResultSet rs2 = statement.executeQuery("ZRANGE runoobkey 0 10 WITHSCORES");
//        while (rs2.next()) {
//            LOGGER.log("rs2:" + rs2.getString(0));
//        }
//
//        statement.execute("HMSET myhash field1 field2");
//        ResultSet rs3 = statement.executeQuery("HGETALL myhash");
//        while (rs3.next()) {
//            LOGGER.log("rs3:" + rs3.getString(0));
//        }

//        ResultSet rs4 = statement.executeQuery("get user");
//        while (rs4.next()) {
//            LOGGER.log("rs4:" + rs4.getString(0));
//        }
    }

    public static void logResult(ResultSet rs, String formattedMsg) throws SQLException {
       int row = 0;
       ResultSetMetaData metadata = rs.getMetaData();
       while (rs.next()) {
           Map<String, Object> values = new LinkedHashMap<>();
           for (int i = 1; i <= metadata.getColumnCount(); i++) {
               values.put(metadata.getColumnName(i), rs.getObject(i));
           }
           LOGGER.log(formattedMsg + "[" + row +"]=%s", values);
           row++;
       }
    }
}

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
class User {
    private String name;
    private Short age;
    private String email;
    private BigDecimal money;

}
