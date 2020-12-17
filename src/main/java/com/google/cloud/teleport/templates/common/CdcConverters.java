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

package com.google.cloud.teleport.templates.common;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

/** Common code for Teleport JdbcToBigQuery. */
public class CdcConverters {

  /** Interface used by the JdbcToBigQuery pipeline to accept user input. */
  public interface CdcToTextOptions extends PipelineOptions {

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
