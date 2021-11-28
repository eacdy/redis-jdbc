package com.itmuch.redis.jdbc;

public interface RedisClient {
    String[] sendCommand(String sql);
    void select(int dbIndex);
    void close();
}
