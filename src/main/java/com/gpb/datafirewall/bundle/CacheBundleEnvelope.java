package com.gpb.datafirewall.bundle;

/**
 * Flink POJO for two broadcast events: full rule/policy snapshot or a lightweight
 * routing-only handler switch. A handler event never contains bytecode.
 */
public final class CacheBundleEnvelope {
    public boolean handlerOnly;
    public String handler;
    public String rulesVersion;
    public String politicsVersion;
    public String checksum;
    public byte[] bytes;

    public CacheBundleEnvelope() { }

    public static CacheBundleEnvelope fromBundle(CacheBundleState bundle) {
        CacheBundleEnvelope result = new CacheBundleEnvelope();
        result.handlerOnly = false;
        result.handler = bundle.handler();
        result.rulesVersion = bundle.compiledRulesVersion();
        result.politicsVersion = bundle.politicsVersion();
        result.checksum = bundle.checksum();
        result.bytes = CacheBundleStateSerializer.encode(bundle);
        result.validateHeader();
        return result;
    }

    public static CacheBundleEnvelope forHandler(String handler) {
        CacheBundleEnvelope result = new CacheBundleEnvelope();
        result.handlerOnly = true;
        result.handler = handler;
        result.validateHeader();
        return result;
    }

    public void validateHeader() {
        if (!"flink".equals(handler) && !"dotnet".equals(handler)) {
            throw new IllegalArgumentException("Unsupported cache handler: " + handler);
        }
        if (handlerOnly) {
            if (rulesVersion != null || politicsVersion != null || checksum != null || bytes != null) {
                throw new IllegalArgumentException("Handler-only update must not contain a cache bundle");
            }
            return;
        }
        if (rulesVersion == null || politicsVersion == null || checksum == null || checksum.isBlank()
                || bytes == null || bytes.length == 0 || bytes.length > 256 * 1024 * 1024) {
            throw new IllegalArgumentException("Invalid encoded cache bundle");
        }
    }
}
