package com.itmuch.redis.jdbc;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisDriver implements Driver {
    private final static Logger LOGGER = new Logger(RedisDriver.class);

    private static final String REDIS_JDBC_PREFIX = "jdbc:redis:";

    private static final AtomicBoolean initialed = new AtomicBoolean(false);

    JedisPool jp;

    static {
        try {
            DriverManager.registerDriver(new RedisDriver());
        } catch (Exception e) {
            LOGGER.log("Can't register driver!");
            throw new RuntimeException("Can't register driver!", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!this.acceptsURL(url)) {
            LOGGER.log("wrong url. url is %s", url);
            return null;
        }
        if (info == null) {
            info = new Properties();
        }

        // remove prefix so we can use URI parsing.
        String rawUrl = url.replaceFirst("jdbc:", "");
        String host = "localhost";
        int port = 3306;
        int database = 0;
        try {
            URI uri = new URI(rawUrl);

            host = uri.getHost() != null ? uri.getHost() : host;
            port = uri.getPort() > 0 ? uri.getPort() : port;

            String path = uri.getPath();
            if (path != null && path.length() > 1) {
                database = Integer.parseInt(path.replaceAll("/", ""));
            }
            // TODO 密码支持/超时时间支持
            int timeout = 1000;
            boolean ssl = false;
            String user = null;
            String password = null;


            final Jedis jedis = new Jedis(host, port, timeout, timeout, ssl);
            jedis.connect();

            if (user != null) {
                jedis.auth(user, password);
            } else if (password != null) {
                jedis.auth(password);
            }
            if (database != 0) {
                jedis.select(database);
            }
//            if (clientName != null) {
//                jedis.clientSetname(clientName);
//            }

            return new RedisConnection(new JedisRedisClient(jedis), database + "", info);
        } catch (URISyntaxException | NumberFormatException e) {
            LOGGER.log("Cannot parse JDBC URL");
            throw new SQLException("Cannot parse JDBC URL: " + url, e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.toLowerCase().startsWith(REDIS_JDBC_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // ref: com.mysql.cj.jdbc.NonRegisteringDriver.getParentLogger
        LOGGER.log("getParentLogger not implemented");
        throw new SQLFeatureNotSupportedException("getParentLogger not implemented");
    }
}
