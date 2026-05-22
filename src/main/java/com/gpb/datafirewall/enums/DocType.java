package com.gpb.datafirewall.enums;

import java.util.Map;
import java.util.Optional;

public enum DocType {
    BIRTH_CERTIFICATE("27"),
    PASSPORT_RU("21"),
    VISA("3"),
    OVERSEAS_PASSPORT_RU("22"),
    DIPLOMATIC_PASSPORT_RU("22"),
    TEMPORARY_CERTIFICATE("26"),
    MILITARY_TICKET("25"),
    OFFICER_CERTIFICATE("24"),
    OFFICE_PASSPORT_RU("0"),
    PASSPORT_FOREIGNER("31"),
    RESIDENCY("33"),
    MIGRATION_CARD("39"),
    TEMPORARY_RESIDENCE("34"),
    PATENT("49");
    
    private static final Map<String, String> TYPE_DUL_MAPPING = Map.ofEntries(
        Map.entry("BIRTH_CERTIFICATE", "27"),
        Map.entry("PASSPORT_RU", "21"),
        Map.entry("VISA", "3"),
        Map.entry("OVERSEAS_PASSPORT_RU", "22"),
        Map.entry("DIPLOMATIC_PASSPORT_RU", "22"),
        Map.entry("TEMPORARY_CERTIFICATE", "26"),
        Map.entry("MILITARY_TICKET", "25"),
        Map.entry("OFFICER_CERTIFICATE", "24"),
        Map.entry("OFFICE_PASSPORT_RU", "0"),
        Map.entry("PASSPORT_FOREIGNER", "31"),
        Map.entry("RESIDENCY", "33"),
        Map.entry("MIGRATION_CARD", "39"),
        Map.entry("TEMPORARY_RESIDENCE", "34"),
        Map.entry("PATENT", "49")
    );
    
    private final String code;
    
    DocType(String code) {
        this.code = code;
    }
    
    /**
     * Самый производительный метод - без создания Optional
     * Возвращает null если ключ не найден
     */
    public static String map(String name) {
        return TYPE_DUL_MAPPING.get(name);
    }
    
    /**
     * Если нужна обработка отсутствующего ключа
     */
    public static Optional<String> mapOptional(String name) {
        return Optional.ofNullable(TYPE_DUL_MAPPING.get(name));
    }
    
    public String getCode() {
        return code;
    }
}