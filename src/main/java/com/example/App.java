// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.csv.CsvIO;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.serialization.StringDeserializer;
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
                                DESCRIPTOR(EVENT_TIMESTAMP),
                                "INTERVAL 5 SECOND",
                                "INTERVAL 1 MINUTE"
                            )
                        ), TRX_DETAILS_WINDOWED AS (
                            SELECT
                                *,
                                QTY*PRICE AS TOTAL
                            FROM HOP(
                                (SELECT * FROM TRX_DETAILS),
                                DESCRIPTOR(EVENT_TIMESTAMP),
                                "INTERVAL 5 SECOND",
                                "INTERVAL 1 MINUTE"
                            )
                        ), TRX_DETAILS_AGG AS (
                            SELECT
                                TRX_ID,
                                SUM(TOTAL) AS TOTAL
                            FROM TRX_DETAILS_WINDOWED
                            GROUP BY TRX_ID
                        )
                        SELECT
                            A.WINDOW_END as __TIME,
                            A.USER_ID,
                            COUNT(A.TRX_ID) AS VOLUME,
                            SUM(B.TOTAL) AS TOTAL
                        FROM TRX_WINDOWED A
                        LEFT JOIN TRX_DETAILS_AGG B
                            ON A.TRX_ID=B.TRX_ID
                        GROUP BY
                            A.WINDOW_END,
                            A.USER_ID
                            """))
                .apply(CsvIO
                        .writeRows("data/csv/", CSVFormat.DEFAULT)
                        .withWindowedWrites()
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    private static <K> PCollection<Row> deserializeKafkaJson(
            PCollection<KafkaRecord<K, String>> pCollection,
            String topic,
            Schema.Builder schemaBuilder) {
        var schema = schemaBuilder.addDateTimeField("event_timestamp").build();
        return pCollection
                .apply(Filter.by(k -> k.getTopic().equals(topic)))
                .apply(MapElements.via(new SimpleFunction<KafkaRecord<?, String>, String>() {
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