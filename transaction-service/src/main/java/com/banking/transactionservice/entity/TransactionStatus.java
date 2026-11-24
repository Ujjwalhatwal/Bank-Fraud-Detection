package com.banking.transactionservice.entity;


/**
 * transaction lifecycle flow :
 * PENDING -> PROCESSING -> COMPLETED (clean transaction)
 *                       -> PENDING_VERIFICATION -> COMPLETED (suspicious detected transaction)
 *                              -> COMPLETED (verified)
 *                              -> FLAGGED (SAGA refund)
 *                       -> FAILED (failed transaction)
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    PENDING_VERIFICATION,
    FLAGGED,
    FAILED
}
