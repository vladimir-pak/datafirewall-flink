package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.services.MessageRecord;

/** Every call reads at most one message and finishes exactly one local broker transaction. */
public interface TransactionBroker extends AutoCloseable {

    /**
     * Blocking receive and one local transaction. Processing failures are logged and committed
     * without reply; cancellation and broker failures roll back. No error queue is used.
     */
    TransactionalReply poll(Processor processor) throws Exception;

    @FunctionalInterface
    interface Processor {
        TransactionalReply process(MessageRecord message) throws Exception;
    }

    @Override
    void close();
}
