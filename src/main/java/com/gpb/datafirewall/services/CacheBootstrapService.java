package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheManifest;
import com.gpb.datafirewall.config.IgniteRulesApiClient;
import com.gpb.datafirewall.converter.CachePayloadConverter;
import com.gpb.datafirewall.dto.CacheResponseDto;
import com.gpb.datafirewall.dto.HttpBytecodeSource;
import com.gpb.datafirewall.ignite.BytecodeSource;

import java.util.Map;
import java.util.Objects;

/** Called only by CacheSnapshotSource. Loads a named, immutable manifest epoch. */
public final class CacheBootstrapService {
    private final IgniteRulesApiClient api;

    private final CachePayloadConverter converter = 
            new CachePayloadConverter(new ObjectMapper());

    private final BytecodeSource bytecodeSource;

    private final boolean requirePublishedChecksum;

    public CacheBootstrapService(
            IgniteRulesApiClient api, 
            BytecodeSource bytecodeSource,
            boolean requirePublishedChecksum) {

        this.api = Objects.requireNonNull(api);
        this.bytecodeSource = Objects.requireNonNull(bytecodeSource);
        this.requirePublishedChecksum = requirePublishedChecksum;
    }

    public static CacheBootstrapService http(
            IgniteRulesApiClient api,
            boolean requirePublishedChecksum) {

        return new CacheBootstrapService(
                api, 
                new HttpBytecodeSource(api), 
                requirePublishedChecksum
        );
    }

    public CacheBundleState load(Long requestedEpoch) {
        CacheManifest manifest = api.getBundleManifest(requestedEpoch);
        return load(manifest);
    }

    public CacheBundleState loadLatest() {
        return load((Long) null);
    }

    public CacheBundleState load(CacheManifest manifest) {
        String rulesVersion = manifest.compiledRulesVersion();
        String politicsVersion = manifest.politicsVersion();

        Map<String,byte[]> bytecodes = 
                bytecodeSource.loadAll("compiled_rules_" + rulesVersion);

        if (bytecodes == null || bytecodes.isEmpty())
            throw new IllegalStateException(
                    "Empty compiled rules for epoch " + manifest.cacheEpoch()
            );

        CacheBundleState bundle = CacheBundleState.create(
                manifest.cacheEpoch(), 
                rulesVersion, 
                politicsVersion, 
                bytecodes,
                converter.toStringMap(
                        cache("politics_dataset2control_area_", politicsVersion), 
                        "dataset2controlArea"
                ),
                converter.toNestedRulesMap(
                        cache("politics_control_area_rules_", politicsVersion), 
                        "controlAreaRules"
                ),
                converter.toNestedStringMap(
                        cache("politics_error_messages_", politicsVersion), 
                        "errorMessages"
                ),
                converter.toSetMap(
                        cache("politics_dataset_exclusion_", politicsVersion), 
                        "datasetExclusion"
                ),
                converter.toBooleanMap(
                        cache("politics_filter_flag_", politicsVersion), 
                        "filterFlag"
                ),
                converter.toStringMap(
                        cache("gender_mapping_", politicsVersion), 
                        "genderMapping"
                ),
                converter.toStringMap(
                        cache("type_dul_mapping_", politicsVersion), 
                        "typeDulMapping"
                ),
                manifest.handler()
        );

        bundle.verify();

        if (requirePublishedChecksum 
                && (manifest.checksum() == null || manifest.checksum().isBlank()))
            throw new IllegalStateException("Published bundle checksum missing, epoch " + manifest.cacheEpoch());

        if (manifest.checksum() != null && !manifest.checksum().isBlank()
                && !manifest.checksum().equalsIgnoreCase(bundle.checksum()))
            throw new IllegalStateException("Published checksum mismatch, epoch " + manifest.cacheEpoch()
                    + " expected=" + manifest.checksum() + " actual=" + bundle.checksum());

        // Validate the same classes, referenced fields and cross-cache links as every worker.
        new RulesCacheRuntime(null, null).prepare(bundle);

        return bundle;
    }

    private Map<String,Object> cache(String prefix,String version) {
        String fullName=prefix + version;
        CacheResponseDto<String,Object> response=api.getVersionedCache(fullName);
        if (response == null || response.getCache() == null)
            throw new IllegalStateException("Missing cache " + fullName);
        if (!fullName.equals(response.getCacheName()))
            throw new IllegalStateException("API returned unexpected cacheName " + response.getCacheName()
                    + " expected=" + fullName);
        return response.getCache();
    }
}
