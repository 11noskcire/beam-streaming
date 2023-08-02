// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.gson.Gson;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Instant;

public class App {
    public interface Options extends StreamingOptions {
        @Description("Input text to print.")
        @Default.String("My input text")
        String getInputText();

        void setInputText(String value);
    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options
                .as(BeamSqlPipelineOptions.class)
                .setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");
        Pipeline pipeline = Pipeline.create(options);
        var kafkaRecords = pipeline.apply("Read from kafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers("kafka:9092")
                        .withTopics(List.of("transactions", "transaction-details"))
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class));
        var transactions = deserializeKafkaJson(
                kafkaRecords,
                "transactions",
                Schema.builder()
                        .addStringField("trx_id")
                        .addStringField("user_id")
                        .addDateTimeField("created_date"));
        var transactionDetails = deserializeKafkaJson(
                kafkaRecords,
                "transaction-details",
                Schema.builder()
                        .addStringField("trx_id")
                        .addStringField("product_id")
                        .addInt64Field("qty")
                        .addDecimalField("price"));

        PCollectionTuple
                .of(new TupleTag<>("TRX"), transactions)
                .and(new TupleTag<>("TRX_DETAILS"), transactionDetails)
                .apply(SqlTransform.query("""
                        WITH TRX_WINDOWED AS (
                            SELECT *
                            FROM HOP(
                                (SELECT * FROM TRX),
                                DESCRIPTOR(event_timestamp),
                                "INTERVAL 5 SECOND",
                                "INTERVAL 1 MINUTE"
                            )
                        ), TRX_DETAILS_WINDOWED AS (
                            SELECT
                                *,
                                qty*price AS total
                            FROM HOP(
                                (SELECT * FROM TRX_DETAILS),
                                DESCRIPTOR(event_timestamp),
                                "INTERVAL 5 SECOND",
                                "INTERVAL 1 MINUTE"
                            )
                        ), TRX_DETAILS_AGG AS (
                            SELECT
                                trx_id,
                                SUM(total) AS total
                            FROM TRX_DETAILS_WINDOWED
                            GROUP BY trx_id
                        )
                        SELECT
                            STRING(A.window_end) AS __time,
                            A.user_id,
                            COUNT(A.trx_id) AS volume,
                            SUM(B.total) AS total
                        FROM TRX_WINDOWED A
                        LEFT JOIN TRX_DETAILS_AGG B
                            ON A.trx_id=B.trx_id
                        GROUP BY
                            A.window_end,
                            A.user_id
                            """))
                .apply("RowToJson", MapElements.via(new SimpleFunction<Row, String>() {
                    @Override
                    public String apply(Row row) {
                        Gson gson = new Gson();
                        Map<String, Object> map = new HashMap<String, Object>();
                        for (String fieldName : row.getSchema().getFieldNames()) {
                            Object value = row.getValue(fieldName);
                            map.put(fieldName, value);
                        }
                        String json = gson.toJson(map);
                        return json;
                    }
                }))
                .apply("WriteToKafka", KafkaIO.<Void, String>write()
                        .withBootstrapServers("kafka:9092")
                        .withTopic("results")
                        .withValueSerializer(StringSerializer.class)
                        .values());

        pipeline.run().waitUntilFinish();
    }

    private static <K> PCollection<Row> deserializeKafkaJson(
            PCollection<KafkaRecord<K, String>> pCollection,
            String topic,
            Schema.Builder schemaBuilder) {
        var schema = schemaBuilder.addDateTimeField("event_timestamp").build();
        return pCollection
                .apply("FilterTopic", Filter.by(k -> k.getTopic().equals(topic)))
                .apply("AddEventTimestamp", MapElements.via(new SimpleFunction<KafkaRecord<?, String>, String>() {
                    @Override
                    public String apply(KafkaRecord<?, String> k) {
                        String json = k.getKV().getValue().trim();
                        json = String.format("%s, \"event_timestamp\": \"%s\"}",
                                json.substring(0, json.length() - 1),
                                new Instant(k.getTimestamp()).toString());
                        return json;
                    }
                }))
                .apply(JsonToRow.withSchema(schema));
    }
}