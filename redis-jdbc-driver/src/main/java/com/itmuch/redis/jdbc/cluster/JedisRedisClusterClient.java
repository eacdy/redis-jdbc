package com.itmuch.redis.jdbc.cluster;

import com.itmuch.redis.jdbc.AbstractRedisClient;
import com.itmuch.redis.jdbc.conf.Hint;
import com.itmuch.redis.jdbc.conf.Op;
import lombok.RequiredArgsConstructor;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

@RequiredArgsConstructor
public class JedisRedisClusterClient extends AbstractRedisClient {
    private final JedisCluster jedisCluster;

    @Override
    protected Object sendCommand(Op op) {
        String rawSql = op.getOriginSql();
        String commandString = op.getCommand();
        String[] params = op.getParams();
        List<Hint> hints = op.getHints();

        try {
            Protocol.Command command = this.convertCommand(commandString);

            String sampleKey = hints.stream()
                    .filter(hint -> Objects.equals(hint.getKey(), Hint.HINT_KEY_SAMPLE_KEY))
                    .findFirst()
                    .map(Hint::getValue)
                    .orElse(null);

            Object result;
            if (params == null || params.length == 0) {
                result = this.jedisCluster.sendCommand(sampleKey, command);
            } else {
                result = this.jedisCluster.sendCommand(sampleKey, command, params);
            }
            return result;
        } catch (Throwable e) {
            LOGGER.log("command `%s` cannot execute.", rawSql);
            throw new RuntimeException(String.format("command `%s` cannot execute.", rawSql));
        }
    }

    @Override
    public void select(int dbIndex) throws SQLException {
        throw new SQLException("Redis Cluster does not support this operation");
    }

    @Override
    public int getDbIndex() throws SQLException {
        return 0;
    }

    @Override
    public boolean hasMultiDb() {
        return false;
    }

    @Override
    public void close() {
        this.jedisCluster.close();
    }
}
