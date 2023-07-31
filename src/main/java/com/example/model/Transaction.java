package com.example.model;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

@DefaultSchema(JavaFieldSchema.class)
public class Transaction {
    public String trx_id;
    public String user_id;
    public Instant created_date;
    public Instant process_date;
}
