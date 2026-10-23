package com.example.kafkaecommerce.user.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "users")
public class UserEntity {
    @Id
    private UUID id;
    @Column(nullable = false, unique = true)
    private String email;
    @Column(nullable = false)
    private String fullName;
    @Column(nullable = false)
    private Instant createdAt;

    protected UserEntity() {
    }

    public UserEntity(UUID id, String email, String fullName) {
        this.id = id;
        this.email = email;
        this.fullName = fullName;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getEmail() { return email; }
    public String getFullName() { return fullName; }
    public Instant getCreatedAt() { return createdAt; }
    public void update(String email, String fullName) { this.email = email; this.fullName = fullName; }
}
