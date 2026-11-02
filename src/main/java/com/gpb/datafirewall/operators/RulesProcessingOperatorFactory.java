package com.gpb.datafirewall.operators;

import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.services.RulesProcessingFunction;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import org.apache.flink.streaming.api.operators.*;

/** Supplies only the task mailbox. No OperatorCoordinator, RPC events or global activation. */
public final class RulesProcessingOperatorFactory extends AbstractStreamOperatorFactory<ProcessingResult>
        implements TwoInputStreamOperatorFactory<MessageRecord, CacheBundleEnvelope, ProcessingResult> {
    private final String jwt;
    private final VaultSecretsDto secrets;
    private final int maxPending;
    private final long maxPendingBytes;

    public RulesProcessingOperatorFactory(String jwt, VaultSecretsDto secrets, int maxPending, long maxPendingBytes) {
        if (maxPending <= 0 || maxPendingBytes <= 0) throw new IllegalArgumentException("Invalid startup buffer limits");
        this.jwt = jwt; this.secrets = secrets;
        this.maxPending = maxPending; this.maxPendingBytes = maxPendingBytes;
        setChainingStrategy(ChainingStrategy.HEAD);
    }
    @Override @SuppressWarnings("unchecked")
    public <T extends StreamOperator<ProcessingResult>> T createStreamOperator(StreamOperatorParameters<ProcessingResult> p) {
        RulesProcessingOperator operator = new RulesProcessingOperator(
                new RulesProcessingFunction(jwt, secrets), p.getMailboxExecutor(), maxPending, maxPendingBytes);
        operator.setProcessingTimeService(p.getProcessingTimeService());
        operator.setup(p.getContainingTask(), p.getStreamConfig(), p.getOutput());
        return (T) operator;
    }
    @Override public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return RulesProcessingOperator.class;
    }
}
