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
import org.apache.beam.sdk.io.csv.CsvIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Instant;

import com.example.model.Transaction;
import com.example.model.TransactionDetail;
import com.example.utility.Gson;

public class App {
    public interface Options extends StreamingOptions {
        @Description("Input text to print.")
        @Default.String("My input text")
        String getInputText();

        void setInputText(String value);
    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);
        var kafkaRecords = pipeline.apply("Read from kafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers("kafka:9092")
                        .withTopics(List.of("transactions", "transaction-details"))
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class));
        PCollection<Transaction> transactions = kafkaRecords
                .apply("FilterTopic", Filter.by(kafkaRecord -> kafkaRecord.getTopic().equals("transactions")))
                .apply("DeserializeJson", MapElements
                        .via(new SimpleFunction<KafkaRecord<String, String>, Transaction>() {
                            @Override
                            public Transaction apply(KafkaRecord<String, String> kafkaRecord) {
                                Gson gson = new Gson();
                                String json = kafkaRecord.getKV().getValue();
                                Transaction t = gson.fromJson(json, Transaction.class);
                                t.process_date = new Instant(kafkaRecord.getTimestamp());
                                return t;
                            }
                        }));
        PCollection<TransactionDetail> transactionDetails = kafkaRecords
                .apply("FilterTopic", Filter.by(kafkaRecord -> kafkaRecord.getTopic().equals("transaction-details")))
                .apply("DeserializeJson", MapElements
                        .via(new SimpleFunction<KafkaRecord<String, String>, TransactionDetail>() {
                            @Override
                            public TransactionDetail apply(KafkaRecord<String, String> kafkaRecord) {
                                Gson gson = new Gson();
                                String json = kafkaRecord.getKV().getValue();
                                TransactionDetail td = gson.fromJson(json, TransactionDetail.class);
                                td.process_date = new Instant(kafkaRecord.getTimestamp());
                                return td;
                            }
                        }));

        var transactionsWindowed = transactions
                .apply(SqlTransform
                        .query("""
                                	select
                                		trx_id,
                                		user_id,
                                		created_date,
                                		process_date
                                	from PCOLLECTION
                                	group by
                                		trx_id,
                                		user_id,
                                		created_date,
                                		process_date,
                                		tumble(process_date, interval '1' minutes)
                                """));

        var transactionDetailsWindowed = transactionDetails
                .apply(SqlTransform
                        .query("""
                                	select
                                		trx_id,
                                		product_id,
                                		qty,
                                		price,
                                		process_date
                                	from PCOLLECTION
                                	group by
                                		trx_id,
                                		product_id,
                                		qty,
                                		price,
                                		process_date,
                                		tumble(process_date, interval '1' minutes)
                                """));

        PCollectionTuple
                .of(new TupleTag<>("trx"), transactionsWindowed)
                .and(new TupleTag<>("trx_details"), transactionDetailsWindowed)
                .apply(SqlTransform.query("""
                        select a.user_id, a.trx_id, sum(b.qty*b.price) as total
                        from trx a
                        left join trx_details b
                        on a.trx_id=b.trx_id
                        group by
                        	a.user_id,
                        	a.trx_id
                        """))
                .apply(CsvIO
                        .writeRows("csv/data", CSVFormat.DEFAULT)
                        .withWindowedWrites()
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

}