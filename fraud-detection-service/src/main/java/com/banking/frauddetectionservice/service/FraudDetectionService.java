package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class FraudDetectionService {

    @Autowired
    private AccountServiceClient accountServiceClient;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransactionsPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    public void checkTransaction(Map<String, Object> payload) {
        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

        // fetch the real balance from Account Service
        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);
        log.info("checking transaction : {} ,account: {} ,amount {}",transactionId, accountNumber, amount);

        FraudCheckResult result  = performFraudCheck(accountNumber, amount, senderBalance);

        if(result.isFraud()){
            log.info("Suspicious activity detected - account: {} , and reason : {}" +
                    "  - requesting OTP verification",accountNumber, result.getReason());

            Map<String,Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId,verificationEvent);
        }else{
            // transaction is clean
            log.info("Transaction is clean");

            Map<String,Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("accountNumber", accountNumber);
            transactionCleanEvent.put("amount", amount);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", "Transaction is clean");

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId,transactionCleanEvent);
        }

    }

    private FraudCheckResult performFraudCheck(
            String accountNumber,
            BigDecimal amount,
            BigDecimal senderBalance) {

        // Pattern 1 : Velocity Check
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(true, "Too many transactions in 60 seconds" +
                    " - Transaction velocity limit exceeded");
        }

        // Pattern 2 : Amount Check
        if(isAmountSuspicious(accountNumber,amount)){
            return new FraudCheckResult(true, "Unusually large transaction amount " +
                    "- Transaction limit exceeded 3x your average");
        }

        // Pattern 3 : Balance Check
        if(senderBalance.compareTo(BigDecimal.ZERO) > 0 && isBalanceCheckFailed(senderBalance, amount)){
            return new FraudCheckResult(true, "Transaction exceeds 90% of your account balance " +
                    "- Balance check failed");
        }
        return new FraudCheckResult(false, "Transaction is clean");
    }

    private boolean isVelocityExceeded(String accountNumber) {
        String key = "fraud:velocity:" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);
        if(count != null && count == 1) {
            // Set expiration for the first increment
            redisTemplate.expire(key, Duration.ofSeconds(60));
        }
        log.info("Velocity check account: {} ,count: {}/{}",
                accountNumber, count, maxTransactionsPerMinute);

        return count != null && count > maxTransactionsPerMinute;
        // Exceeded the allowed number of transactions in 60 seconds
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        String avgkey = "fraud:avg_amount:" + accountNumber;
        String avgAmountStr = redisTemplate.opsForValue().get(avgkey);

        if(avgAmountStr == null){
            // If no average amount is stored, consider the first transaction as normal
            redisTemplate.opsForValue().set(avgkey, amount.toString());
            return false;
        }
        BigDecimal avgAmount = new BigDecimal(avgAmountStr);
        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier)); // 5x

        // Update the average amount with the new transaction : update running avg
        BigDecimal newAvgAmount = avgAmount.add(amount).divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
        redisTemplate.opsForValue().set(avgkey, newAvgAmount.toString());

        log.info("Amount check - amount: {}, threshold: {}, suspicious: {}",
                amount, threshold, amount.compareTo(threshold) > 0);
        return amount.compareTo(threshold) > 0;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {

        BigDecimal maxAllowed = senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage)); // 90% of balance
        log.info("Balance check - amount: {}, maxAllowed: {}, suspicious: {}",
                amount, maxAllowed, amount.compareTo(maxAllowed) > 0);
        return amount.compareTo(maxAllowed) > 0;
    }
}
