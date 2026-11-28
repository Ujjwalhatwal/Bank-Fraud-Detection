package com.banking.frauddetectionservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class FraudDetectionEventConsumer {

    @Autowired
    private FraudDetectionService fraudDetectionService;

    /**
     *Listen to transaction.Initiated topic
     * Every transaction goes through fraud check before completing.
     * @param payload The message payload containing transaction details.
     */
    @KafkaListener(topics = "transaction.initiated")
    public void consumeTransactionInitiated(@Payload Map<String, Object> payload){
        log.info("Received Transaction for fraud check with transactionId: {}", payload.get("transactionId"));

        try{
            fraudDetectionService.checkTransaction(payload);
        }catch(Exception e){
            log.error("Error processing transaction for fraud check with transactionId: {}", payload.get("transactionId"), e);
        }
    }

}
