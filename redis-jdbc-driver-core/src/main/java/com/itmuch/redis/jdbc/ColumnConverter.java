package com.itmuch.redis.jdbc;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;

@AllArgsConstructor
@RequiredArgsConstructor
@Getter
@SuperBuilder
public class ColumnConverter<R> {

    public static final ColumnConverter TTL_CONVERTER = ColumnConverter.<Timestamp>builder()
           .columnName("TIMESTAMP")
           .targetType(Timestamp.class)
           .columnTypeName(Types.TIMESTAMP)
           .converter(str -> {
               Long number = Long.parseLong(str);
               if (number == -1 || number == -2) {
                   return null;
               }
               LocalDateTime ldt = LocalDateTime.now().plus(number, ChronoUnit.SECONDS).truncatedTo(ChronoUnit.SECONDS);
               return Timestamp.valueOf(ldt);
           })
           .build();

    public static final ColumnConverter PTTL_CONVERTER = ColumnConverter.<Timestamp>builder()
            .columnName("TIMESTAMP")
            .targetType(Timestamp.class)
            .columnTypeName(Types.TIMESTAMP)
            .converter(str -> {
                Long number = Long.parseLong(str);
                if (number == -1 || number == -2) {
                    return null;
                }
                LocalDateTime ldt = LocalDateTime.now().plus(number, ChronoUnit.MILLIS).truncatedTo(ChronoUnit.SECONDS);
                return Timestamp.valueOf(ldt);
            })
            .build();

    protected static final Map<String, ColumnConverter> COMMAND_CONVERTERS = Utils.imapOf(
            "TTL", TTL_CONVERTER,
            "PTTL", PTTL_CONVERTER
                                                                                         );

    Function<String, R> converter;

    BiFunction<String, String[], R> inputAwareConverter;

    Class<R> targetType;

    Integer columnTypeName;

    String columnName;

    public BiFunction<String, String[], R> getInputAwareConverter() {
        if (inputAwareConverter == null) {
            inputAwareConverter = ((String value, String[] args) -> this.converter.apply(value));
        }
        return inputAwareConverter;
    }

}
