// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.google.gson.JsonSyntaxException;

public class App {
	static final TupleTag<Transaction> parsedMessages = new TupleTag<Transaction>() {
	};
	static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
	};

	public interface Options extends StreamingOptions {
		@Description("Input text to print.")
		@Default.String("My input text")
		String getInputText();

		void setInputText(String value);
	}

	public static void main(String[] args) {
		var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		var pipeline = Pipeline.create(options);
		var parseResults = pipeline
				/**
				 * DAG: read
				 */
				.apply(
						"Read from kafka topic",
						KafkaIO.<String, String>read()
								.withBootstrapServers("kafka:9092")
								.withTopic("source")
								.withKeyDeserializer(StringDeserializer.class)
								.withValueDeserializer(StringDeserializer.class)
								.withoutMetadata())
				// .apply(
				// "Generate",
				// Create.of(
				// KV.<String, String>of(null,
				// "{ \"trx_id\": \"1\", \"user_id\": \"2\", \"timestamp\":
				// \"2023-07-31T01:12:30+07\", \"comment\": \"asdf\" }"),
				// KV.<String, String>of(null,
				// "{ \"trx_id\": \"1\", \"user_id\": \"2\", \"timestamp\":
				// \"2023-07-31T01:12:30+07\", \"comment\": \"asdf\" }")))
				/**
				 * DAG: read ┬──> unparsed
				 * ********* │
				 * ********* └──> parsed
				 */
				.apply("Parse json", ParDo.of(new DoFn<KV<String, String>, Transaction>() {
					@ProcessElement
					public void processElement(ProcessContext context) {
						var element = context.element();
						MyGson gson = new MyGson();
						String json = element.getValue();
						try {
							Transaction data = gson.fromJson(json, Transaction.class);
							context.output(parsedMessages, data);
						} catch (JsonSyntaxException e) {
							context.output(unparsedMessages, json);
						}
					}
				}).withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));

		parseResults.get(unparsedMessages)
				/**
				 * DAG: read ┬──> unparsed ──> window
				 * ********* │
				 * ********* └──> parsed
				 */
				.apply(
						"Fixed Window every 60s",
						Window.into(FixedWindows.of(Duration.standardMinutes(1))))
				/**
				 * DAG: read ┬──> unparsed ──> window ──> write
				 * ********* │
				 * ********* └──> parsed
				 */
				.apply(
						"Write unparsed data to file",
						TextIO.write()
								.to("unparsed/transaction")
								.withWindowedWrites()
								.withNumShards(1).withSuffix(".json"));

		parseResults.get(parsedMessages)
				/**
				 * DAG: read ┬──> unparsed ──> fixedWindow ──> write
				 * ********* │
				 * ********* └──> parsed ──> userId+timestamp ──> sessionWindow
				 */
				.apply(WithTimestamps.of((Transaction t) -> t.timestamp)
						.withAllowedTimestampSkew(Duration.millis(Long.MAX_VALUE)))
				// .apply(ParDo.of(new DoFn<Transaction, Transaction>() {
				// 	@ProcessElement
				// 	public void processElement(ProcessContext context) {
				// 		var element = context.element();
				// 		var ts = context.timestamp();
				// 		MyGson gson = new MyGson();
				// 		System.out.println(gson.toJson(element));
				// 		context.output(element);
				// 	}
				// }))
				.apply(
						"window",
						Window.<Transaction>into(
								Sessions.withGapDuration(Duration.standardSeconds(30)))
								.withAllowedLateness(Duration.standardMinutes(1))
								.withTimestampCombiner(TimestampCombiner.EARLIEST))
				.apply(WithKeys.of((Transaction t) -> t.userId).withKeyType(TypeDescriptors.strings()))
				.apply(GroupByKey.create())
				.apply(ParDo.of(new DoFn<KV<String, Iterable<Transaction>>, KV<String, Integer>>() {
					@ProcessElement
					public void processElement(ProcessContext context) {
						var element = context.element();
						var x = context.timestamp();
						String userId = element.getKey();
						MyGson gson = new MyGson();
						int count = 0;
						Iterable<Transaction> transactions = element.getValue();
						for (Transaction transaction : transactions) {
							System.out.println("\n\n\n" + gson.toJson(transaction) + "\n\n\n");
							count++;
						}
						System.out.println(userId + "\t" + count);
						context.output(KV.of(userId, count));
					}
				}));

		pipeline.run().waitUntilFinish();
	}
}