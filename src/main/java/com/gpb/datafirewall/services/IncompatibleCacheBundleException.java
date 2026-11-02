package com.gpb.datafirewall.services;

/** Valid individual cache versions that cannot yet be activated together. */
public final class IncompatibleCacheBundleException extends IllegalStateException {
    public IncompatibleCacheBundleException(String message) {
        super(message);
    }
}
