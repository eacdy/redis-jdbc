package com.itmuch.redis.jdbc;

import java.sql.SQLException;

public interface RedisClient {
    String[] sendCommand(String sql) throws SQLException;

    void select(int dbIndex) throws SQLException;

    int getDbIndex() throws SQLException;

    void close();
}
