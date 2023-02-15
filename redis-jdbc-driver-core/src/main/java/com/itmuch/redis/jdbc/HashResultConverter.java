package com.itmuch.redis.jdbc;

import java.util.Map;
import java.util.function.BiFunction;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.ArrayUtils;

@AllArgsConstructor
@RequiredArgsConstructor
@Getter
@SuperBuilder
public class HashResultConverter {

    public static final HashResultConverter KEY_EVEN_VALUE_ODD_ARRAY = HashResultConverter.builder()
           .keyExtractor(((String[] results, String[] args) -> {
                return subArray(results, true);
            }))
           .valueExtractor(((String[] results, String[] args) -> {
               return subArray(results, false);
            }))
           .name("KEY_EVEN_VALUE_ODD_ARRAY")
                                                                                          .build();

    public static final HashResultConverter ARGS_ARE_KEY_NAMES = HashResultConverter.builder()
           .keyExtractor(((String[] results, String[] args) -> ArrayUtils.subarray(args, 1, args.length)))
           .valueExtractor(((String[] results, String[] args) -> results))
           .name("ARGS_ARE_KEY_NAMES")
            .build();


    protected static final Map<String, HashResultConverter> COMMAND_CONVERTERS = Utils.imapOf(
            "HGETALL", KEY_EVEN_VALUE_ODD_ARRAY,
            "HMGET", ARGS_ARE_KEY_NAMES,
            "HGET", ARGS_ARE_KEY_NAMES
                                                                                               );

    BiFunction<String[], String[], String[]> keyExtractor;
    BiFunction<String[], String[], String[]> valueExtractor;

    String name;

    private static String[] subArray(String[] source, boolean even) {
        if (source.length %2 != 0) {
            throw new IllegalArgumentException("Expecting array to be kw pairs");
        }
        String[] result = new String[source.length/2];
        for (int i = 0; i < source.length; i++) {
            boolean isEven = i %2 == 0;
            if (even && isEven) {
                result[i/2] = source[i];
            } else if(!even && !isEven) {
                result[(i-1)/2] = source[i];
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return this.getClass().toString() + '@' + name;
    }
}
