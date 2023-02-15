package com.itmuch.redis.jdbc.conf;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;

@AllArgsConstructor
@Getter
public enum Feature {
    EXTRA_COLUMN_CONVERSIONS("extraColumnConversion", false),
    HASH_RESULT_CONVERSIONS("hashResultConversion", false);
    String propName;
    boolean defaultValue;

    public static Map<Feature, Boolean> fromProperties(Map properties) {
        return Arrays.stream(values())
                .collect(Collectors.toMap(e -> e, e -> {
                    String value = properties.getOrDefault(e.propName, Boolean.toString(e.defaultValue)).toString();
                    return BooleanUtils.toBoolean(value);
                }));
    }
}
