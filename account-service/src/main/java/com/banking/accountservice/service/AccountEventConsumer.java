package com.banking.accountservice.service;

import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
public class AccountEventConsumer {

    @Autowired
    private AccountService accountService;

    /**
     * This method is a Kafka consumer that listens for transaction completed events.
     * no fraud detected, then the transaction is completed and the account balance is updated.
     * credit receiver account
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){

        try{

            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} with amount: {}", receiverAccount, amount);
            accountService.creditBalance(receiverAccount, amount);
        }catch (Exception e){
            log.error("Error while processing transaction completed event: {}", e.getMessage());
        }
    }

    /**
     * This method is a Kafka consumer that listens for fraud detected events.
     * If fraud is detected, the account is blocked.
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Blocking account: {} due to fraud detection", accountNumber);
            accountService.blockAccount(accountNumber);
        }catch (Exception e){
            log.error("Error while processing fraud detected event: {}", e.getMessage());
        }
    }
}
