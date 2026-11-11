package com.gpb.datafirewall.transaction.kafka;

import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.RulesProcessingFunction;
import com.gpb.datafirewall.transaction.TransactionSettings;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

/** Each parallel subtask owns exactly one local MQ/JMS transaction session. */
public final class TransactionalRulesOperatorFactory
        extends AbstractStreamOperatorFactory<ProcessingResult>
        implements OneInputStreamOperatorFactory<CacheBundleEnvelope, ProcessingResult> {

    private final String jwt;
    private final VaultSecretsDto secrets;
    private final TransactionSettings settings;
    private final AuditConfig auditConfig;

    public TransactionalRulesOperatorFactory(
            String jwt, VaultSecretsDto secrets, TransactionSettings settings,
            AuditConfig auditConfig) {
        this.jwt = jwt;
        this.secrets = secrets;
        this.settings = settings;
        this.auditConfig = auditConfig;
        setChainingStrategy(ChainingStrategy.HEAD);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<ProcessingResult>> T createStreamOperator(
            StreamOperatorParameters<ProcessingResult> parameters) {
        TransactionalRulesOperator operator = new TransactionalRulesOperator(
                new RulesProcessingFunction(jwt, secrets),
                parameters.getMailboxExecutor(), settings, auditConfig);
        operator.setProcessingTimeService(parameters.getProcessingTimeService());
        operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return TransactionalRulesOperator.class;
    }
}
