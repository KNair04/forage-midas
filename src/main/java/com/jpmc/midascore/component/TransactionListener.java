package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @Autowired
    private RestTemplate restTemplate;

    private static final String INCENTIVE_URL = "http://localhost:8085/incentive";

    @KafkaListener(topics = "${midas.kafka.topic}", groupId = "midas-consumer")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        processTransaction(transaction);
    }

    @Transactional
    public void processTransaction(Transaction transaction) {
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        Optional<UserRecord> senderOpt = userRepository.findById(senderId);
        Optional<UserRecord> recipientOpt = userRepository.findById(recipientId);

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            logger.warn("Invalid transaction: sender={} or recipient={} not found", senderId, recipientId);
            return;
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        if (sender.getBalance() < amount) {
            logger.warn("Invalid transaction: insufficient funds for sender {} (balance={}, amount={})",
                    senderId, sender.getBalance(), amount);
            return;
        }

        float incentiveAmount = 0f;
        try {
            Incentive response = restTemplate.postForObject(INCENTIVE_URL, transaction, Incentive.class);
            if (response != null && response.getAmount() >= 0) {
                incentiveAmount = response.getAmount();
            }
        } catch (Exception e) {
            logger.error("Incentive API call failed: {}. Proceeding with incentive=0", e.getMessage());
        }

        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record = new TransactionRecord(sender, recipient, amount, incentiveAmount);
        transactionRecordRepository.save(record);

        logger.info("Transaction completed: sender={} -> recipient={} | amount={} | incentive={}",
                senderId, recipientId, amount, incentiveAmount);


        if (recipient.getName().equalsIgnoreCase("wilbur")) {
            System.out.println("DEBUG: Wilbur's updated balance = " + recipient.getBalance());
        }
    }
}
