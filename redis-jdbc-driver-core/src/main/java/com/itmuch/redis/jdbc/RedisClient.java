package com.itmuch.redis.jdbc;

import java.sql.SQLException;
import java.util.Map;

import com.itmuch.redis.jdbc.conf.Feature;

public interface RedisClient {
    String[] sendCommand(String sql) throws SQLException;

    void select(int dbIndex) throws SQLException;

    int getDbIndex() throws SQLException;

    void close();

    Map<Feature, Boolean> getFeatureMap();

    default boolean hasMultiDb() {
        return true;
    }
}
