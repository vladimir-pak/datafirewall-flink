package com.gpb.datafirewall.config;

/**
 * Сохранение HTTP-статусов при использовании обёрток загрузчиков
 * В случае неустранимых ошибок повторные попытки не должны продолжаться бесконечно.
 */
public final class CacheApiException extends RuntimeException {
    private final int status;
    public CacheApiException(int status, String url) {
        super("Cache API HTTP " + status + " for " + url);
        this.status = status;
    }
    public int status() { return status; }
    public boolean versionUnavailable() { return status == 404 || status == 410; }
    public boolean retryable() { return status == 408 || status == 429 || status >= 500; }
    public static CacheApiException find(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof CacheApiException api) return api;
        }
        return null;
    }
}
