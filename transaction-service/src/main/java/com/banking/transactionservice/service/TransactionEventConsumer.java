package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TransactionEventConsumer {

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    public static final long OTP_EXPIRATION_MINUTES = 5;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(@Payload Map<String, Object> payload) {

            try{
                String transactionId = (String) payload.get("transactionId");
                String accountNumber = (String) payload.get("accountNumber");
                String reason = (String) payload.get("reason");
                log.info("Verification required for transactionId: {}, reason: {}", transactionId, reason);

                Transaction transaction = transactionRepository.findById(transactionId)
                        .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));

                // idempotency check: only work for processing status
                if(transaction.getTransactionStatus() != TransactionStatus.PROCESSING) {
                    log.warn("Transaction with id: {} is not in PROCESSING status." +
                            " Current status: {}. Skipping verification required processing.",
                            transactionId, transaction.getTransactionStatus()
                    );
                    return;
                }

                // generate 6 digit OTP
                String otp = String.format("%06d", (int)(Math.random() * 900000) + 100000);

                // store the OTP in redis - expire in 5 minutes
                String optKey = "verification:otp:" + transactionId;
                redisTemplate.opsForValue().set(optKey, otp, OTP_EXPIRATION_MINUTES , TimeUnit.MINUTES); // expire in 5 minutes

                // update transaction status to PENDING_VERIFICATION
                transaction.setTransactionStatus(TransactionStatus.PENDING_VERIFICATION);
                transactionRepository.save(transaction);
                log.info("OTP generated for transaction : {} expires in {} minutes.", transactionId, OTP_EXPIRATION_MINUTES);

                // Notify user
                Map<String, Object> optEvent = new HashMap<>();
                optEvent.put("transactionId", transactionId);
                optEvent.put("accountNumber", accountNumber);
                optEvent.put("reason", reason);
                optEvent.put("otp", otp);
                optEvent.put("amount", payload.get("amount"));

                kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, optEvent);


            } catch (Exception e) {
                log.error("Error handling verification required : {}", e.getMessage());

            }
    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanEvent(@Payload Map<String,Object> payload){

        try{

            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);

        } catch (Exception e) {
            log.error("Error handling fraud check clean result : {}", e.getMessage());
        }
    }
}
