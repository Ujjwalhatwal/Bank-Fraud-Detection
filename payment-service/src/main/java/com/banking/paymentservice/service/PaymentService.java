package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    @Autowired
    private PaymentRepository  paymentRepository;

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${razorpay.keyId}")
    private String keyId;
    @Value("${razorpay.keySecret}")
    private String keySecret;

    private final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private final String PAYMENT_FAILED_TOPIC = "payment.failed";


    /**
     * Creates Razorpay payment order.
     * FLOW :
     * 1. Create a payment order in Razorpay using the provided request details.
     * 2. Save the payment order details in the database.
     * 3. Return the payment order details to the client/frontend
     * 4. frontend show razorpay checkout page.
     * 5. user pays
     * 6. razorpay calls webhook
     * @param request
     * @return
     */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment order for account: {}, amount: {}",
                request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

        // converted amount
        int convertedAmount = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount); // amount in the smallest currency unit
        orderRequest.put("currency", "INR");
        orderRequest.put("receipt", "order_rcptid_"+
                System.currentTimeMillis() + UUID.randomUUID().toString()
        .replace("-", "").substring(0, 10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}",razorpayOrder.get("id").toString());


        // 2. Save the payment order details in the database
        Payment payment = new Payment();
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("INR");
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());
        Payment savePayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savePayment.getId(),
                razorpayOrder.get("id").toString(),
                savePayment.getAmount(),
                savePayment.getCurrency(),
                savePayment.getStatus(),
                savePayment.getDescription(),
                keyId
        );
    }

    public void handleWebHook(Map<String, Object> payload) {
        log.info("Received webhook event: {}",payload.get("event"));

        String event = (String) payload.get("event");

        if("payment.captured".equals((event))){
            handlePaymentSuccess(payload);
        }
        else if("payment.failed".equals((event))){
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {

        try{

            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() ->
                            new RuntimeException("Payment not found for orderId: " + orderId));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            // Publish payment completed event to Kafka
            Map<String, Object> eventPayload = new HashMap<>();
            eventPayload.put("paymentId", payment.getId());
            eventPayload.put("accountNumber", payment.getAccountNumber());
            eventPayload.put("amount", payment.getAmount());
            eventPayload.put("razopayPaymentId", payment.getRazorpayPaymentId());


            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId() ,eventPayload);
            log.info("Published payment completed event to Kafka for paymentId: {}", payment.getId());


        } catch (Exception e) {
            log.error("Error handling payment success webhook: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try{

            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() ->
                            new RuntimeException("Payment not found for orderId: " + orderId));
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            // Publish payment failed event to Kafka
            Map<String, Object> eventPayload = new HashMap<>();
            eventPayload.put("paymentId", payment.getId());
            eventPayload.put("accountNumber", payment.getAccountNumber());
            eventPayload.put("amount", payment.getAmount());
            eventPayload.put("reason",payment.getFailureReason());

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId() ,eventPayload);
            log.warn("Published payment failed event to Kafka for paymentId: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error handling payment failure webhook: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");
        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");
        return (Map<String,Object>) paymentWrapper.get("entity");
    }


}
