package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.services.MessageRecord;

/** Every call reads at most one message and finishes exactly one local broker transaction. */
public interface TransactionBroker extends AutoCloseable {

    /** On a business processing error the original request is moved to the broker error queue atomically. */
    TransactionalReply poll(Processor processor) throws Exception;

    @FunctionalInterface
    interface Processor {
        TransactionalReply process(MessageRecord message) throws Exception;
    }

    @Override
    void close();
}
