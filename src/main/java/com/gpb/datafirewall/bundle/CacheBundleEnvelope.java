package com.gpb.datafirewall.bundle;

/** Flink POJO: a complete encoded snapshot, serialized once before the broadcast edge. */
public final class CacheBundleEnvelope {
    public long epoch;
    public String checksum;
    public byte[] bytes;

    public CacheBundleEnvelope() { }

    public static CacheBundleEnvelope fromBundle(CacheBundleState bundle) {
        CacheBundleEnvelope result = new CacheBundleEnvelope();
        result.epoch = bundle.cacheEpoch();
        result.checksum = bundle.checksum();
        result.bytes = CacheBundleStateSerializer.encode(bundle);
        result.validateHeader();
        return result;
    }

    public void validateHeader() {
        if (epoch < 0 || checksum == null || checksum.isBlank() || bytes == null
                || bytes.length == 0 || bytes.length > 256 * 1024 * 1024)
            throw new IllegalArgumentException("Invalid encoded cache bundle");
    }
}
