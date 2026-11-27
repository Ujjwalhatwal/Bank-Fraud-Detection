package com.banking.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationEventConsumer {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGeneratedEvent(@Payload Map<String, Object> payload) {

        try{

            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(
                    accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            """
                                    Suspicious activity detected on your account %s.
                                     Reason: %s.
                                     A transaction of amount: %s is pending verification.
                                     Your OTP is %s. Valid for 5 minutes.
                                     Please verify the transaction using the OTP.
                                     If you did not initiate this transaction, please contact our support immediately.""",
                            accountNumber, reason, amount, otp
                    )
            );
        } catch (Exception e) {
            log.error("Error sending OTP notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompletedEvent(@Payload Map<String,Object> payload){

        try{
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            // debit alert to sender
            sendAlert(
                    senderAccount,
                    "DEBIT ALERT",
                    String.format(
                            "A transaction of amount: %s has been debited from your account %s.\n"
                            + " If you did not authorize this transaction, please contact our support immediately.",
                            amount, senderAccount
                    )
            );

            // credit alert to receiver
            sendAlert(
                    receiverAccount,
                    "CREDIT ALERT",
                    String.format(
                            "A transaction of amount: %s has been credit to your account %s.\n"
                                    + " If you did not authorize this transaction, please contact our support immediately.",
                            amount, receiverAccount
                    )
            );

        } catch (Exception e) {
            log.error("Error sending transaction completed notification: {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetectedEvent(@Payload Map<String,Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(
                    accountNumber,
                    "SUSPICIOUS ACTIVITY ALERT",
                    String.format(
                            """
                                    Suspicious activity detected on your account %s.
                                     Reason: %s.
                                     Your account has been bloacked.
                                     Please contact bank support immediately.""",
                            accountNumber, reason
                    )
            );

        } catch (Exception e) {
            log.error("Error sending fraud detected notification: {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionReFundedEvent(@Payload Map<String,Object> payload){

        try{
            String senderAccount = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(
                    senderAccount,
                    "REFUND PROCESSED",
                    String.format(
                            """
                                    Your transaction of amount: %s was cancelled due to reason: %s.
                                     The amount has been refunded to your account %s.
                                     Please contact bank support if you have any questions.""",
                            amount, reason, senderAccount
                    )
            );

        } catch (Exception e) {
            log.error("Error sending refund notification: {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompletedEvent(@Payload Map<String,Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(
                    accountNumber,
                    "PAYMENT SUCCESSFUL",
                    String.format(
                           "A payment of amount: %s has been completed successfully.\n"
                            + "RazorpayId is %s.",
                            amount, payload.get("razorpayPaymentId")
                    )
            );

        } catch (Exception e) {
            log.error("Error sending payment received notification: {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailedEvent(@Payload Map<String,Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(
                    accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment of amount: %s could not be processed.\n"
                            + "Please try again or contact support.",
                            amount
                    )
            );

        } catch (Exception e) {
            log.error("Error sending payment failed notification: {}", e.getMessage());
        }

    }

    private void sendAlert(String accountNumber, String subject, String format) {
        log.info("----------------------------------------------------------------------");
        log.info("Account: {}", accountNumber);
        log.info("Subject: {}", subject);
        log.info("Message: {}", format);
        log.info("----------------------------------------------------------------------");
    }
    

}
