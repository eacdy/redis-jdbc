package com.itmuch.redis.jdbc;

import java.util.Arrays;
import java.util.List;
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

    public static Op parseSql(String sql) {
        if (sql.contains("SELECT 'keep alive'")) {
            return new Op("PING", new String[0]);
        }

        String[] arr = sql.split(" ");

        String commandString = arr[0];

        if (arr.length == 1) {
            return new Op(commandString, new String[0]);
        } else {
            String[] commandParams = Arrays.copyOfRange(arr, 1, arr.length);
            return new Op(commandString, commandParams);
        }
    }
}
