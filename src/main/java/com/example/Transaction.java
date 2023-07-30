package com.example;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

import com.google.gson.annotations.SerializedName;

@DefaultSchema(JavaFieldSchema.class)
public class Transaction {
    @SerializedName("trx_id")
    String transactionId;
    @SerializedName("user_id")
    String userId;
    Instant timestamp;
}
