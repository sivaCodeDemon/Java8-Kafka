package com.example.kafkaecommerce.events;

public final class Topics {
    public static final String USER_CREATED = "user.created.v1";
    public static final String USER_UPDATED = "user.updated.v1";
    public static final String ORDER_CREATED = "order.created.v1";
    public static final String ORDER_CANCELLED = "order.cancelled.v1";
    public static final String ORDER_STATUS = "order.status.v1";
    public static final String INVENTORY_RESERVED = "inventory.reserved.v1";
    public static final String INVENTORY_REJECTED = "inventory.rejected.v1";
    public static final String INVENTORY_RELEASED = "inventory.released.v1";
    public static final String PAYMENT_SUCCEEDED = "payment.succeeded.v1";
    public static final String PAYMENT_FAILED = "payment.failed.v1";
    public static final String PAYMENT_REFUNDED = "payment.refunded.v1";
    public static final String PAYMENT_COUNTS = "payment.success-counts.v1";
    public static final String TRANSACTION_DEMO = "demo.transactional.v1";

    private Topics() {
    }
}
