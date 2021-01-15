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
import com.google.cloud.teleport.util.ResourceUtils;
import io.debezium.connector.mysql.MySqlConnector;
import org.apache.beam.io.cdc.DebeziumIO;
import org.apache.beam.io.cdc.SourceRecordJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * A template that copies CDC data from a relational database using JDBC to a BigQuery table.
 */
public class CdcToBigQuery {

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

        // Step 2: Transform Debezium event (Json) into TableRow, single field: json.
        .apply("Json to TableRow", ParDo.of(new DoFn<String, TableRow>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            c.output(new TableRow().set("json", c.element()));
        }}))

        // Step 3: Append TableRow to a given BigQuery table: outputTable argument.
        .apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .withoutValidation()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withJsonSchema(ResourceUtils.getCdcTableSchemaJson())
//                        .withSchema(getTableSchema())
                        .to(options.getOutputTable()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}

/************* Comentario para Chaparro ********************/

/** Interface used by the CdcToBigQuery pipeline to accept user input. */
interface CdcToBigQueryOptions extends PipelineOptions {
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
