package com.example.model;

import java.math.BigDecimal;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

@DefaultSchema(JavaFieldSchema.class)
public class TransactionDetail {
    public String trx_id;
    public String product_id;
    public int qty;
    public BigDecimal price;
    public Instant process_date;
}
