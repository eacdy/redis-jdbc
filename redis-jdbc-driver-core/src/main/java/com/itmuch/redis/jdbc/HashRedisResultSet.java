package com.itmuch.redis.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.function.Function;

public class HashRedisResultSet extends RedisResultSet {
    private final static Logger LOGGER = new Logger(HashRedisResultSet.class);

    private final String[] keys;
    private final String[] values;

    public HashRedisResultSet(String[] result, final Statement owningStatement, String[] commandArguments, HashResultConverter converter) throws SQLException {
        super(converter.keyExtractor.apply(result, commandArguments), owningStatement, commandArguments, null);
        this.keys = converter.keyExtractor.apply(result, commandArguments);
        this.values = converter.valueExtractor.apply(result, commandArguments);
        if (keys.length != values.length) {
            throw new SQLException("Result cannot be converted");
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HashRedisResultSetMetaData(Arrays.asList("KEY", "VALUE"));
    }

    @Override
    protected <T> T getColumnIndexWithDefault(int columnIndex, T nullDefault, Function<String, T> converter) throws SQLException {
        String str;
        if (columnIndex == 1) {
            str = keys[getPosition()];
        } else if (columnIndex == 2) {
            str = values[getPosition()];
        } else {
            throw new SQLException("Invalid columnIndex " + columnIndex);
        }
        if (str == null) {
            return nullDefault;
        }
        return converter.apply(str);
    }
}
