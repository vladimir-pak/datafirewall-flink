
package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import java.util.function.Consumer;

public interface TransactionBroker extends AutoCloseable {
    TransactionalReply poll(Processor processor, Consumer<ProcessingResult> errorAudit) throws Exception;
    @FunctionalInterface
    interface Processor {
        TransactionalReply process(MessageRecord message) throws Exception;
    }
    @Override
    void close();
}
