package com.gpb.datafirewall.coordination;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.services.RulesProcessingFunction;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;

/** Flink 1.20 factory; Job содержит 1 координатор и N подзадач (subtasks). */
public final class RulesProcessingOperatorFactory 
        extends AbstractStreamOperatorFactory<ProcessingResult>
        implements CoordinatedOperatorFactory<ProcessingResult>,
                   TwoInputStreamOperatorFactory<MessageRecord, CacheBundleState, ProcessingResult> {
                    
    private final String jwt;
    private final VaultSecretsDto secrets;
    private final int maxPending;
    private final long maxPendingBytes;

    public RulesProcessingOperatorFactory(
            String jwt, 
            VaultSecretsDto secrets,
            int maxPending, 
            long maxPendingBytes) {
        if (maxPending <= 0 || maxPendingBytes <= 0) 
            throw new IllegalArgumentException("Invalid cache buffer limits");

        this.jwt = jwt; this.secrets = secrets;
        this.maxPending = maxPending; this.maxPendingBytes = maxPendingBytes;
        setChainingStrategy(ChainingStrategy.HEAD);
    }

    @Override @SuppressWarnings("unchecked")
    public <T extends StreamOperator<ProcessingResult>> T createStreamOperator(
            StreamOperatorParameters<ProcessingResult> p) {
        OperatorID id = p.getStreamConfig().getOperatorID();

        RulesProcessingOperator operator = 
                new RulesProcessingOperator(
                        new RulesProcessingFunction(jwt, secrets),
                        p.getOperatorEventDispatcher().getOperatorEventGateway(id),
                        p.getMailboxExecutor(), 
                        maxPending, 
                        maxPendingBytes
                );

        operator.setProcessingTimeService(p.getProcessingTimeService());
        operator.setup(p.getContainingTask(), p.getStreamConfig(), p.getOutput());
        p.getOperatorEventDispatcher().registerEventHandler(id, operator);
        return (T) operator;
    }

    @Override public OperatorCoordinator.Provider getCoordinatorProvider(
            String name, OperatorID id) {
        return new CacheActivationCoordinator.Provider(id);
    }

    @Override public Class<? extends StreamOperator> getStreamOperatorClass(
            ClassLoader classLoader) {
        return RulesProcessingOperator.class;
    }
}
