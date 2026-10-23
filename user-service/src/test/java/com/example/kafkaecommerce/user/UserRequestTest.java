package com.example.kafkaecommerce.user;

import com.example.kafkaecommerce.user.dto.UserRequest;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UserRequestTest {
    @Test
    void validatesEmailAndName() {
        var validator = Validation.buildDefaultValidatorFactory().getValidator();
        assertTrue(validator.validate(new UserRequest("a@example.com", "Alex")).isEmpty());
        assertFalse(validator.validate(new UserRequest("not-an-email", "")).isEmpty());
    }
}
