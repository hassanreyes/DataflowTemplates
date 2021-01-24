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

import com.google.cloud.teleport.cdc.LAJsonToTableRow;
import com.google.cloud.teleport.cdc.CdcPipelineOptions;
import com.google.cloud.teleport.util.ResourceUtils;
import io.debezium.connector.mysql.MySqlConnector;
import org.apache.beam.io.cdc.DebeziumIO;
import org.apache.beam.io.cdc.DebeziumSDFDatabaseHistory;
import org.apache.beam.io.cdc.SourceRecordJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies CDC data from a relational database using JDBC to a BigQuery table.
 */
public class LosAngelesToBigQuery {
  private static final Logger LOG = LoggerFactory.getLogger(LosAngelesToBigQuery.class);

  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    CdcPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(CdcPipelineOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(CdcPipelineOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Step 1: Configure Debezium connector
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
                                              .withConnectionProperty("database.server.name", "dbserver1")
                                              .withConnectionProperty("database.include.list", "inventory")
                                              .withConnectionProperty("table.include.list", "inventory.la_online_orders")
                                              // DebeziumSDFDatabaseHistory is the default database history
                                              .withConnectionProperty("database.history", DebeziumSDFDatabaseHistory.class.getName())
                                              .withConnectionProperty("include.schema.changes", "false")
                                              .withConnectionProperty("database.allowPublicKeyRetrieval", "true")
                              ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper()).withCoder(StringUtf8Coder.of())
              ).setCoder(StringUtf8Coder.of())

              // Step 2: Transform Debezium event (Json) into TableRow, single field: json.

                .apply("Json to TableRow",
                        ParDo.of(new LAJsonToTableRow()))

            // Step 3: Append TableRow to a given BigQuery table: outputTable argument.
            .apply(
                    "Write to BigQuery",
                    BigQueryIO.writeTableRows()
                            .withoutValidation()
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                            .withJsonSchema(ResourceUtils.getCdcTableSchemaJson())
                            .to(options.getOutputTable()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}




