package com.gpb.datafirewall.enums;

import java.util.Map;
import java.util.Optional;

public enum Gender {
    MALE("M"),
    FEMALE("F");
    
    private static final Map<String, String> SEX_MAPPING = Map.of(
        "MALE", "M",
        "FEMALE", "F"
    );
    
    private final String code;
    
    Gender(String code) {
        this.code = code;
    }

    public static String map(String name) {
        return SEX_MAPPING.get(name);
    }
    
    /**
     * Если нужна обработка отсутствующего ключа
     */
    public static Optional<String> mapOptional(String name) {
        return Optional.ofNullable(SEX_MAPPING.get(name));
    }
    
    public String getCode() {
        return code;
    }
}
