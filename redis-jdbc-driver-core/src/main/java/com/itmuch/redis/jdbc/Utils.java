package com.itmuch.redis.jdbc;

import com.itmuch.redis.jdbc.conf.Hint;
import com.itmuch.redis.jdbc.conf.Op;

import java.io.BufferedReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {
    public static boolean isNumber(String str) {
        if (str == null || str.length() == 0) {
            return false;
        }
        for (int i = 0; i < str.length(); i++) {
            boolean digit = Character.isDigit(str.charAt(i));
            if (!digit) {
                return false;
            }
        }
        return true;
    }

    public static <T> List<T> toList(T[] arr) {
        if (arr == null) {
            return null;
        }
        return Arrays.stream(arr)
                .collect(Collectors.toList());
    }

    public static Op parseSql(String rawSql, Set<String> allowedHintKeys) {
        if (allowedHintKeys == null || allowedHintKeys.size() == 0) {
            allowedHintKeys = Hint.DEFAULT_ALLOWED_KEYS;
        }

        // for IDEA database tool only
        if (rawSql.contains("SELECT 'keep alive'")) {
            return new Op(rawSql, null, "PING", new String[0]);
        }

        // hints
        List<String> lines = new BufferedReader(new StringReader(rawSql))
                .lines()
                .collect(Collectors.toList());

        List<String> hintLines = new ArrayList<>();
        List<String> sqlLines = new ArrayList<>();
        lines.forEach(line -> {
            if (line.startsWith("--")) {
                hintLines.add(line);
            } else {
                sqlLines.add(line);
            }
        });

        Set<String> finalAllowedHintKeys = allowedHintKeys;
        List<Hint> hints = hintLines
                .stream()
                .map(line -> {
                    String hintStr = line.replace("--", "")
                            .replaceAll(" ", "");
                    String[] arr = hintStr.split(":");

                    boolean contains = finalAllowedHintKeys.contains(arr[0]);
                    String hintKey = contains ? arr[0] : "noop";

                    return new Hint(hintKey, arr[1]);
                }).collect(Collectors.toList());


        // sql to execute
        StringBuilder sb = new StringBuilder();
        sqlLines.forEach(sb::append);

        String sql = sb.toString();

        String[] arr = sql.split(" ");

        String commandString = arr[0];

        if (arr.length == 1) {
            return new Op(rawSql, hints, commandString, new String[0]);
        } else {
            String[] commandParams = Arrays.copyOfRange(arr, 1, arr.length);
            return new Op(rawSql, hints, commandString, commandParams);
        }
    }

    public static Map<String, String> parseQueryStringToMap(String queryString) {
        String[] params = queryString.split("&");
        Map<String, String> map = new HashMap<>();
        for (String param : params) {
            String[] p = param.split("=");
            if (p.length == 2) {
                map.put(p[0], p[1]);
            }
        }
        return map;
    }

    public static <E extends Enum<E>> E findEnum(final Class<E> enumClass, final String enumName) {
        try {
            return Enum.valueOf(enumClass, enumName);
        } catch (final IllegalArgumentException ex) {
            return null;
        }
    }

    public static List<byte[]> convert(Collection<?> collection, List<byte[]> list) throws SQLException {
        for (Object t : collection) {
            if (t == null) {
                list.add(null);
            } else if (t instanceof byte[]) {
                list.add((byte[]) t);
            } else if (t instanceof Collection) {
                List<byte[]> decode = convert((Collection<?>) t, new ArrayList<>());
                list.addAll(decode);
            } else {
                throw new SQLException("Cannot deserialize.");
            }
        }
        return list;
    }
}
