package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    @Autowired
    private TransactionRepository transactionRepository;

    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String,Object> kafkaTemplate;

    private final RedisTemplate<String,String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    /**
     * Step 1 : saga pattern
     * deduct from sender via feign
     * save transaction as PROCESSING
     * publish event to Kafka for fraud check
     * return transaction response with status PROCESSING
     * @param transferRequest
     * @return
     */
    public TransactionResponse transfer(@Valid TransferRequest transferRequest) {

        log.info("SAGA Starts : Transfer from {} to {} amount {}",
                transferRequest.getSenderAccountNumber(),
                transferRequest.getReceiverAccountNumber(),
                transferRequest.getAmount()
        );

        // saga pattern step 1 : deduct from sender via feign
        accountServiceClient.deductBalance(transferRequest.getSenderAccountNumber(), transferRequest.getAmount());

        // save transaction as PROCESSING
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(transferRequest.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(transferRequest.getReceiverAccountNumber());
        transaction.setAmount(transferRequest.getAmount());
        transaction.setTransactionType(TransactionType.TRANSFER);
        transaction.setTransactionStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(transferRequest.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction saveTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING with id: {}", saveTransaction.getId());

        // publish event to Kafka for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent();
        event.setTransactionId(saveTransaction.getId());
        event.setSenderAccountNumber(saveTransaction.getSenderAccountNumber());
        event.setReceiverAccountNumber(saveTransaction.getReceiverAccountNumber());
        event.setAmount(saveTransaction.getAmount());
        event.setDescription(saveTransaction.getDescription());
        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,saveTransaction.getId(), event);
        log.info("SAGA Step 2 : TransactionInitiatedEvent published : {}", saveTransaction.getId());

        return mapToResponse(saveTransaction);

    }

    public TransactionResponse getTransaction(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));
        return mapToResponse(transaction);
    }


    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        List<Transaction> transactionList = transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber);
        return transactionList.stream().map(this::mapToResponse).collect(Collectors.toList());

    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {

        log.info("Verifying OTP for transactionId: {} with otp: {}", transactionId, otp);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));

        String otpKey = "verification:otp:" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            // OTP expired or not found
            log.warn("OTP expired or not found for transactionId: {}", transactionId);
            compensateTransaction(transaction,"OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            // Wrong OTP entered
            // BLOCK account and REFUND
            log.warn("Wrong OTP - BLOCK account and REFUND for transactionId: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensateTransaction(transaction,"Wrong OTP entered - transaction cancelled and account blocked for security");
            return mapToResponse(transaction);
        }

        // OTP correct - complete transaction
        log.info("OTP verified successfully for transactionId: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason) {

        log.warn("SAGA COMPENSATION - refunding {} amount {}",
                transaction.getSenderAccountNumber(),
                transaction.getAmount()
        );
        // credit money back to sender synchronously via feign
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());

        transaction.setTransactionStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + " - SAGA COMPENSATION executed and amount refunded to sender at "+ LocalDateTime.now());
        transactionRepository.save(transaction);

        // Publish refund event to Kafka - Notification service will alert user
        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA COMPENSATION COMPLETED - {} refunded to {}",
                transaction.getAmount(),
                transaction.getSenderAccountNumber()
        );
    }

    private void blockAccountAndCompensateTransaction(Transaction transaction, String reason) {
        // publish fraud.detected event to kafka - Account service will block the account
        Map<String,Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("fraud.detected published account: {} account will be blocked." +
                "Kindly contact to the Bank", transaction.getSenderAccountNumber());
        // saga compensation - refund to sender
        compensateTransaction(transaction, reason);
    }

    private void completeTransaction(Transaction transaction) {
        transaction.setTransactionStatus(TransactionStatus.COMPLETED);
        transaction.setCompetedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent();
        completedEvent.setTransactionId(transaction.getId());
        completedEvent.setSenderAccountNumber(transaction.getSenderAccountNumber());
        completedEvent.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        completedEvent.setAmount(transaction.getAmount());
        completedEvent.setDescription(transaction.getDescription());

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);
        log.info("SAGA COMPLETED - Transaction {} completed successfully.", transaction.getId());
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));

        // idempotency check: only work for processing status
        if(transaction.getTransactionStatus() != TransactionStatus.PROCESSING) {
            log.warn("Transaction with id: {} is not in PROCESSING status." +
                            " Current status: {}. Skipping fraud check clean processing.",
                    transactionId, transaction.getTransactionStatus()
            );
            return;
        }
        completeTransaction(transaction);
    }

    private TransactionResponse mapToResponse(Transaction transaction) {
        TransactionResponse response = new TransactionResponse();
        response.setId(transaction.getId());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setTransactionType(transaction.getTransactionType());
        response.setTransactionStatus(transaction.getTransactionStatus());
        response.setDescription(transaction.getDescription());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setFailureReason(transaction.getFailureReason());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setCompetedAt(transaction.getCompetedAt());
        return response;
    }

}
