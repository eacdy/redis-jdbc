package com.itmuch.redis.jdbc.conf;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Hint {
    public static final String HINT_KEY_DECODER = "decoder";
    public static final String HINT_KEY_SAMPLE_KEY = "sample_key";

    public static final Set<String> DEFAULT_ALLOWED_KEYS = Stream.of(HINT_KEY_DECODER, HINT_KEY_SAMPLE_KEY).collect(Collectors.toSet());

    /**
     * hint key
     */
    private String key;
    /**
     * hint value
     */
    private String value;
}
