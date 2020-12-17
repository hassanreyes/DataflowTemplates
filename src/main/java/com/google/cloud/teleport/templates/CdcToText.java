/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.templates.common.CdcConverters;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.relational.history.MemoryDatabaseHistory;
import org.apache.beam.io.cdc.DebeziumIO;
import org.apache.beam.io.cdc.SourceRecordJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


/**
 * A template that copies CDC data from a relational database using JDBC to a gs location.
 */
public class CdcToText {

  private static ValueProvider<String> maybeDecrypt(
      ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
  }

  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    CdcConverters.CdcToTextOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(CdcConverters.CdcToTextOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(CdcConverters.CdcToTextOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "Read from DebeziumIO",
            DebeziumIO.<String>read()
                .withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                                .withUsername(options.getUsername())
                                .withPassword(options.getPassword())
                                .withConnectorClass(MySqlConnector.class)
                                .withHostName(options.getHostname())
                                .withPort(options.getPort())
                                .withConnectionProperty("database.server.id", "184054")
                                .withConnectionProperty("database.server.name", "dbserver1")
                                .withConnectionProperty("database.include.list", "classicmodels.employees")
                                .withConnectionProperty("database.history", MemoryDatabaseHistory.class.getName())
                                .withConnectionProperty("include.schema.changes", "false")
                                .withConnectionProperty("database.allowPublicKeyRetrieval", "true")
                ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
        ).setCoder(StringUtf8Coder.of())
        .apply("Json to TableRow", ParDo.of(new DoFn<String, TableRow>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output(new TableRow().set("json", c.element()));
        }}))
//        /*
//         * Step 2: Append TableRow to an existing BigQuery table
//         */
        .apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .withoutValidation()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
//                        .withSchema(getTableSchema())
                        .to(options.getOutputTable()));
//          .apply("Write to file",
//                  TextIO.<String>write().to(options.getOutputDirectory()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
