package com.itmuch.redis.jdbc;

import com.itmuch.redis.jdbc.conf.Hint;
import com.itmuch.redis.jdbc.conf.Op;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractRedisClient implements RedisClient {
    public static final Logger LOGGER = new Logger(AbstractRedisClient.class);

    @Override
    public String[] sendCommand(String sql) throws SQLException {
        try {
            Op op = Utils.parseSql(sql, null);

            String firstParam = op.getParams().length == 0 ? null : op.getParams()[0];
            if (op.getCommand().equals("USE") && firstParam != null) { //DB switch
                select(Integer.valueOf(op.getParams()[0]));
                return new String[]{"DB switched to " + op.getParams()[0]};
            } else if (op.getCommand().equals("SELECT") && StringUtils.equalsIgnoreCase(firstParam, "DB_NAME()")) {
                return new String[]{String.valueOf(this.getDbIndex())};
            } else if (op.getCommand().equals("SELECT") && StringUtils.equalsIgnoreCase(firstParam, "keep_alive")) { // for IDEA database tool only
                op = new Op(sql, null, "PING", new String[0]);
            }

            Object result = this.sendCommand(op);

            return this.decodeResult(sql, result, op.getHints());
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }

    protected abstract Object sendCommand(Op op);

    protected Protocol.Command convertCommand(String commandString) {
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

    /**
     * hint:
     * -- decoder:jdk
     * TODO
     *
     * @param sql
     * @param originResult
     * @param hints
     * @return
     */
    protected String[] decodeResult(String sql, Object originResult, List<Hint> hints) throws SQLException {
        String[] decodedResult;
        if (originResult == null) {
            decodedResult = new String[]{null};
        } else if (originResult.getClass().isArray()) {
            String decoded = SafeEncoder.encode((byte[]) originResult);
            decodedResult = Stream.of(decoded)
                    .toArray(String[]::new);
        } else if (originResult instanceof Collection) {
            List<byte[]> convertedList = Utils.convert((Collection<?>) originResult, new ArrayList<>());
            decodedResult = convertedList.stream()
                    .map(SafeEncoder::encode)
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
