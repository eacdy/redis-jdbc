package com.itmuch.redis.jdbc;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
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
               return ttlConvert(str, System.currentTimeMillis(), ChronoUnit.SECONDS);
           })
           .build();

    public static final ColumnConverter PTTL_CONVERTER = ColumnConverter.<Timestamp>builder()
            .columnName("TIMESTAMP")
            .targetType(Timestamp.class)
            .columnTypeName(Types.TIMESTAMP)
            .converter(str -> {
                return ttlConvert(str, System.currentTimeMillis(), ChronoUnit.MILLIS);
            })
            .build();

    public static final ColumnConverter EXPIRETIME_CONVERTER = ColumnConverter.<Timestamp>builder()
            .columnName("TIMESTAMP")
            .targetType(Timestamp.class)
            .columnTypeName(Types.TIMESTAMP)
            .converter(str -> {
                return ttlConvert(str, 0L, ChronoUnit.MILLIS);
            })
            .build();

    protected static final Map<String, ColumnConverter> COMMAND_CONVERTERS = Utils.imapOf(
            "TTL", TTL_CONVERTER,
            "PTTL", PTTL_CONVERTER,
            "EXPIRETIME", EXPIRETIME_CONVERTER
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

    private static Timestamp ttlConvert(String source, long fromEpoch, ChronoUnit unit) {
        Long num = Long.parseLong(source);
        if (num == -1 || num == -2) {
            return null;
        }
        Instant inst = Instant.ofEpochMilli(fromEpoch).plus(num, unit).truncatedTo(ChronoUnit.SECONDS);
        return Timestamp.from(inst);
    }

}
