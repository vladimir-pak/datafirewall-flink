package com.gpb.datafirewall.bundle;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Гарантирует, что в broadcast используется тот же явный сериализатор, что и в управляемом состоянии. */
public final class CacheBundleStateTypeInfo extends TypeInformation<CacheBundleState> {

    private static final long serialVersionUID = 1L;

    public static final CacheBundleStateTypeInfo INSTANCE = new CacheBundleStateTypeInfo();

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<CacheBundleState> getTypeClass() {
        return CacheBundleState.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<CacheBundleState> createSerializer(SerializerConfig c) {
        return new CacheBundleStateSerializer();
    }

    @Override
    @Deprecated
    public TypeSerializer<CacheBundleState> createSerializer(ExecutionConfig c) {
        return new CacheBundleStateSerializer();
    }

    @Override
    public String toString() {
        return "CacheBundleStateTypeInfo(v1)";
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CacheBundleStateTypeInfo;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CacheBundleStateTypeInfo;
    }

    @Override
    public int hashCode() {
        return CacheBundleStateTypeInfo.class.hashCode();
    }
}
