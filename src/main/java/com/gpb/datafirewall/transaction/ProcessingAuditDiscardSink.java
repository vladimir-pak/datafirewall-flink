package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.dto.ProcessingResult;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/** Materialize the transactional stream when its optional Kafka audit branch is disabled. */
@SuppressWarnings("deprecation")
public final class ProcessingAuditDiscardSink extends RichSinkFunction<ProcessingResult> {
    @Override
    public void invoke(ProcessingResult value, Context context) {
        // Broker input and reply are already committed; nothing is required here.
    }
}
