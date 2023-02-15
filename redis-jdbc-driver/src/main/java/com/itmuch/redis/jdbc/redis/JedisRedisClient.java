package com.itmuch.redis.jdbc.redis;

import java.sql.SQLException;
import java.util.Map;

import com.itmuch.redis.jdbc.AbstractRedisClient;
import com.itmuch.redis.jdbc.Logger;
import com.itmuch.redis.jdbc.conf.Feature;
import com.itmuch.redis.jdbc.conf.Op;
import lombok.Getter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class JedisRedisClient extends AbstractRedisClient {
    public static final Logger LOGGER = new Logger(JedisRedisClient.class);

    private final Jedis jedis;

    private int dbIndex;

    @Getter
    private final Map<Feature, Boolean> featureMap;

    public JedisRedisClient(Jedis jedis, int initialDbIndex, Map<Feature, Boolean> featureMap) {
        this.jedis = jedis;
        this.dbIndex = initialDbIndex;
        this.featureMap = featureMap;
    }

    @Override
    protected synchronized Object sendCommand(Op op) {
        String rawSql = op.getOriginSql();
        String commandString = op.getCommand();
        String[] params = op.getParams();

        int db = -1;
        try {
            db = jedis.getDB();
            Protocol.Command command = this.convertCommand(commandString);

            Object result;
            if (params == null || params.length == 0) {
                result = this.jedis.sendCommand(command);
            } else {
                result = this.jedis.sendCommand(command, params);
            }
            return result;
        } catch (Throwable e) {
            LOGGER.log("command on db %s `%s` cannot execute.", db, rawSql);
            throw new RuntimeException(String.format("command on db %s `%s` cannot execute.", db, rawSql));
        }
    }

    @Override
    public synchronized void select(int dbIndex) {
        this.jedis.select(dbIndex);
        this.dbIndex = dbIndex;
    }

    @Override
    public int getDbIndex() throws SQLException {
        return this.dbIndex;
    }

    @Override
    public synchronized void close() {
        LOGGER.log("close()");
        this.jedis.close();
    }
}
