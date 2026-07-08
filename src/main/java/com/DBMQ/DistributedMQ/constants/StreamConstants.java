package com.DBMQ.DistributedMQ.constants;

import java.util.List;

public final class StreamConstants {
    private StreamConstants(){}

    public static final String ORDER_PLACED = "order.placed";
    public static final String EMAIL_GROUP = "email-group";
    public static final String INVENTORY_GROUP = "inventory-group";
    public static final String PAYMENT_GROUP = "payment-group";

    public static final List<String> CONSUMER_GROUPS = List.of(
            EMAIL_GROUP,
            INVENTORY_GROUP,
            PAYMENT_GROUP
    );
}
