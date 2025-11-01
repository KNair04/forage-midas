package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "transactions")
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "sender_id", nullable = false)
    private UserRecord sender;

    @ManyToOne
    @JoinColumn(name = "recipient_id", nullable = false)
    private UserRecord recipient;

    private float amount;

    // NEW: store incentive returned by the incentives API
    private float incentive;

    public TransactionRecord() {}

    // Constructor updated to accept incentive
    public TransactionRecord(UserRecord sender, UserRecord recipient, float amount, float incentive) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
        this.incentive = incentive;
    }

    public Long getId() { return id; }

    public UserRecord getSender() { return sender; }

    public UserRecord getRecipient() { return recipient; }

    public float getAmount() { return amount; }

    public float getIncentive() { return incentive; }

    public void setIncentive(float incentive) { this.incentive = incentive; }

}
