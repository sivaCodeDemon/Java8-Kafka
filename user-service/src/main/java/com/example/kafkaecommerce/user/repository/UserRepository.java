package com.example.kafkaecommerce.user.repository;

import com.example.kafkaecommerce.user.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface UserRepository extends JpaRepository<UserEntity, UUID> {
}
