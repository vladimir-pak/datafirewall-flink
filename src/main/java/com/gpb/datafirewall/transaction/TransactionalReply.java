package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.dto.ProcessingResult;

/** Business processing result collected before the broker transaction commits. */
public record TransactionalReply(
        ProcessingResult reply, 
        ProcessingResult audit) {

    public TransactionalReply {
        if (reply == null 
                || reply.getShortJson() == null 
                || reply.getShortJson().isBlank()
        ) {
            throw new IllegalArgumentException(
                    "Transactional processing must produce a reply"
            );
        }

        if (audit == null) {
            throw new IllegalArgumentException(
                    "Transactional processing must produce an audit result"
            );
        }
    }
}
