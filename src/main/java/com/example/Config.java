package com.example;

import io.github.cdimascio.dotenv.Dotenv;

public class Config {
    private static final Dotenv DOTENV = Dotenv.configure().load();
    public static final String KAFKA_BROKER = DOTENV.get("KAFKA_BROKER");
    public static final String TRANSACTION_TOPIC = DOTENV.get("TRANSACTION_TOPIC");
    public static final String TRANSACTION_DETAIL_TOPIC = DOTENV.get("TRANSACTION_DETAIL_TOPIC");
    public static final String RESULT_TOPIC = DOTENV.get("RESULT_TOPIC");
}
