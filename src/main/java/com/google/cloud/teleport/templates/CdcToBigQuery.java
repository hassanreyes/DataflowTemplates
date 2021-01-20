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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.connector.mysql.MySqlConnector;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.io.cdc.DebeziumIO;
import org.apache.beam.io.cdc.SourceRecordJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
// Import SLF4J packages.
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A template that copies CDC data from a relational database using JDBC to a BigQuery table.
 */
public class CdcToBigQuery {
        @DefaultCoder(AvroCoder.class)
        static class RowElement {
                final String table;
                final String schema;
                final String values;

                public RowElement() {
                        this.table = "";
                        this.schema = "";
                        this.values = "";
                }
                public RowElement(String table, String schema, String values) {
                        this.table = table;
                        this.schema = schema;
                        this.values = values;
                }
        }
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    CdcToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(CdcToBigQueryOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(CdcToBigQueryOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Step 1: Configure Dedezium connector
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
                                // DebeziumSDFDatabaseHistory is the default database history
                                //.withConnectionProperty("database.history", DebeziumSDFDatabaseHistory.class.getName())
                                .withConnectionProperty("include.schema.changes", "false")
                                .withConnectionProperty("database.allowPublicKeyRetrieval", "true")
                ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper()).withCoder(StringUtf8Coder.of())
        ).setCoder(StringUtf8Coder.of())

        // Step 2: Transform Debezium event (Json) into a List of Strings.
        .apply("Json to RowElement", ParDo.of(new DoFn<String, RowElement>() {
          private final Logger LOG = LoggerFactory.getLogger(CdcToBigQuery.class);
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
                try {
                        JsonObject rootObj = JsonParser.parseString(c.element()).getAsJsonObject();
                        JsonObject metadata = rootObj.get("metadata").getAsJsonObject();
                        String values = rootObj.getAsJsonObject("after").get("fields").getAsString();
                        String table = metadata.get("table").getAsString();
                        // TODO: Generate Schema from JSON
                        // TODO: Generate values from JSON
                        RowElement record = new RowElement(
                                table,
                                "{ \"fields\": [{ \"name\": \"json\", \"type\": \"string\", \"mode\": \"required\" }] }",
                                "{ \"json\": " + values + "}"
                        );
                        c.output(record);
                } catch (Exception e) {
                        LOG.error("Error " + e.getMessage());
                }
        }}))

//         // Step 3: Append TableRow to a given BigQuery table: outputTable argument.
//         .apply(
//                 "Write to BigQuery",
//                 BigQueryIO.writeTableRows()
//                         .withoutValidation()
//                         .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                         .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                         .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
//                         // Let BigQuery decides the best method
//                         // .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
//                         .withJsonSchema(ResourceUtils.getCdcTableSchemaJson())
// //                        .withSchema(getTableSchema())
//                         .to(options.getOutputTable()));
       
        // Step 3: Append TableRow to a given BigQuery table: outputTable argument.
        .apply(
                "Write to BigQuery",
                BigQueryIO.<RowElement>write()
                        .to(
                        new DynamicDestinations<RowElement, RowElement>() {
                                @Override
                                public RowElement getDestination(ValueInSingleWindow<RowElement> elem) {
                                        return elem.getValue();
                                }

                                @Override
                                public TableDestination getTable(RowElement row) {
                                        return new TableDestination(
                                        new TableReference()
                                                .setProjectId("apache-beam-poc-4a7e4215")
                                                .setDatasetId("google_beam_poc")
                                                .setTableId(row.table + "_v2"),
                                        "Table for " + row.table);
                                }

                                @Override
                                public TableSchema getSchema(RowElement row) {
                                        TableSchema tableSchema = new TableSchema();
                                        List<TableFieldSchema> fields = new ArrayList<>();

                                        JsonObject rootObj = JsonParser.parseString(row.schema).getAsJsonObject();
                                        JsonArray columns = rootObj.getAsJsonArray("fields");
                                        for (int i = 0; i < columns.size(); i++) {
                                                JsonObject inputField = columns.get(i).getAsJsonObject();
                                                TableFieldSchema field =
                                                new TableFieldSchema()
                                                        .setName(inputField.get("name").getAsString())
                                                        .setType(inputField.get("type").getAsString());

                                                if (inputField.has("mode")) {
                                                        field.setMode(inputField.get("mode").getAsString());
                                                }
                                                fields.add(field);
                                        }
                                        tableSchema.setFields(fields);
                                        return tableSchema;
                                }
                        })
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                        .withoutValidation()
                        .withFormatFunction(
                                (RowElement elem) -> BigQueryConverters.convertJsonToTableRow(elem.values)
                        ));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
  /** Interface used by the CdcToBigQuery pipeline to accept user input. */
public interface CdcToBigQueryOptions extends PipelineOptions {
        @Description(
                "Comma separate list of driver class/dependency jar file GCS paths "
                        + "for example "
                        + "gs://<some-bucket>/driver_jar1.jar,gs://<some_bucket>/driver_jar2.jar")
        ValueProvider<String> getDriverJars();

        void setDriverJars(ValueProvider<String> driverJar);

        @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
        ValueProvider<String> getDriverClassName();

        void setDriverClassName(ValueProvider<String> driverClassName);

        @Description(
                "The JDBC connection Hostname string.")
        ValueProvider<String> getHostname();

        void setHostname(ValueProvider<String> hostname);

        @Description(
                "The JDBC connection Port string.")
        ValueProvider<String> getPort();

        void setPort(ValueProvider<String> port);

        @Description(
                "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
        ValueProvider<String> getConnectionProperties();

        void setConnectionProperties(ValueProvider<String> connectionProperties);

        @Description("JDBC connection user name. ")
        ValueProvider<String> getUsername();

        void setUsername(ValueProvider<String> username);

        @Description("JDBC connection password. ")
        ValueProvider<String> getPassword();

        void setPassword(ValueProvider<String> password);

        @Description("BigQuery table to write to.")
        @Default.String("outputTable")
        @Validation.Required
        ValueProvider<String> getOutputTable();

        void setOutputTable(ValueProvider<String> value);

        @Description("Temporary directory for BigQuery loading process")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
        }
}

/************* Comentario para Chaparro ********************/


