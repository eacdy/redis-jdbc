package com.itmuch.redis.jdbc;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class JedisRedisClient implements RedisClient {
    public static final Logger LOGGER = new Logger(JedisRedisClient.class);

    private final Jedis jedis;

    public JedisRedisClient(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public synchronized String[] sendCommand(String sql) {
        int db = this.jedis.getDB();

        LOGGER.log("sendCommand('%s') on db %s", sql, db);

        try {
            Op op = Utils.parseSql(sql);

            String commandString = op.getCommand();
            String[] params = op.getParams();

            Protocol.Command command = this.convertCommand(commandString);

            Object result;
            if (params == null || params.length == 0) {
                result = this.jedis.sendCommand(command);
            } else {
                result = this.jedis.sendCommand(command, params);
            }
            return this.decodeResult(sql, result);
        } catch (Throwable e) {
            LOGGER.log("command on db %s `%s` cannot execute.", db, sql);
            throw new RuntimeException(String.format("command on db %s `%s` cannot execute.", db, sql));
        }
    }

    @Override
    public synchronized void select(int dbIndex) {
        this.jedis.select(dbIndex);
    }

    @Override
    public synchronized void close() {
        LOGGER.log("close()");
        this.jedis.close();
    }


    private Protocol.Command convertCommand(String commandString) {
        return Arrays.stream(Protocol.Command.values())
                .filter(t -> {
                    String string = t.toString();
                    return string.equalsIgnoreCase(commandString);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        String.format("command invalided. commandString = %s", commandString)
                ));
    }

    private String[] decodeResult(String sql, Object originResult) {
        String[] decodedResult;
        if (originResult == null) {
            decodedResult = new String[]{null};
        } else if (originResult.getClass().isArray()) {
            String decoded = SafeEncoder.encode((byte[]) originResult);
            decodedResult = Stream.of(decoded)
                    .toArray(String[]::new);

        } else if (originResult instanceof Collection) {
            List<?> list = (List<?>) originResult;
            decodedResult = list.stream()
                    .map(t -> SafeEncoder.encode((byte[]) t))
                    .toArray(String[]::new);

        } else {
            LOGGER.log("cannot decode result. originResult = %s", originResult);
            decodedResult = Stream.of(originResult.toString())
                    .toArray(String[]::new);
        }
        LOGGER.log("decode success. sql = %s, originResult = %s, decodedResult = %s",
                sql, originResult, Utils.toList(decodedResult));
        return decodedResult;
    }
}
