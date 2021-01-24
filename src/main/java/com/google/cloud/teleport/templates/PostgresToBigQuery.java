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
import com.google.cloud.teleport.cdc.CdcPipelineOptions;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.connector.postgresql.PostgresConnector;
import org.apache.beam.io.cdc.DebeziumIO;
import org.apache.beam.io.cdc.DebeziumSDFDatabaseHistory;
import org.apache.beam.io.cdc.SourceRecordJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies CDC data from a relational database using JDBC to a BigQuery table.
 */
public class PostgresToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresToBigQuery.class);

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
                                .withConnectorClass(PostgresConnector.class)
                                .withHostName(options.getHostname())
                                .withPort(options.getPort())
                                .withConnectionProperty("tasks.max", "1")
                                .withConnectionProperty("database.dbname", "postgres")
                                .withConnectionProperty("database.server.name", "dbserver2")
//                                .withConnectionProperty("schema.include.list", "inventory")
                                .withConnectionProperty("table.include.list", "inventory.customers")
//                                .withConnectionProperty("slot.name", "debezium")
                                // DebeziumSDFDatabaseHistory is the default database history
                                .withConnectionProperty("database.history", DebeziumSDFDatabaseHistory.class.getName())
//                                .withConnectionProperty("include.schema.changes", "false")
//                                .withConnectionProperty("plugin.name", "decoderbufs")
//                                .withConnectionProperty("heartbeat.interval.ms", "200")
//                                .withConnectionProperty("publication.autocreate.mode", "filtered")
                                .withConnectionProperty("snapshot.mode", "exported")
                ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper()).withCoder(StringUtf8Coder.of())
        ).setCoder(StringUtf8Coder.of())

        // Step 2: Transform Debezium event (Json) into TableRow, single field: json.
        .apply("Json to TableRow", ParDo.of(new DoFn<String, TableRow>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
                JsonObject jsonObject = new JsonParser().parse(c.element()).getAsJsonObject();
                String table = jsonObject.getAsJsonObject().get("metadata").getAsJsonObject().get("table").getAsString();

                LOG.debug("PIPELINE DEBUG: Json -> {}", c.element());

                if(table.contains("customers"))
                {
                    LOG.debug("PIPELINE DEBUG: customers -> {}", c.element());

                    TableRow tr = new TableRow();
                    tr.set("id", jsonObject.getAsJsonObject().get("after").getAsJsonObject().get("fields").getAsJsonObject().get("id").getAsInt());
                    tr.set("first_name", jsonObject.getAsJsonObject().get("after").getAsJsonObject().get("fields").getAsJsonObject().get("first_name").getAsString());
                    tr.set("last_name", jsonObject.getAsJsonObject().get("after").getAsJsonObject().get("fields").getAsJsonObject().get("last_name").getAsString());
                    tr.set("source", "postgres");

                    c.output(tr);
                }
        }}))

        // Step 3: Append TableRow to a given BigQuery table: outputTable argument.
        .apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .withoutValidation()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
//                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withJsonSchema(ResourceUtils.getCdcTableSchemaJson())
                        .to(options.getOutputTable()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}

