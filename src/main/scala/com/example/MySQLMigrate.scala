/*
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/
package com.example

import java.util.Properties

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql._
import com.typesafe.config._
import scala.collection.JavaConverters._
import org.apache.spark.storage.StorageLevel

import scala.util.Try


object MySQLMigrate {

  val log: Logger = LogManager.getRootLogger

  /**
    * Function to extract data from JDBC datasource
    *
    * @return
    */
  def extractFn: (SparkSession, Config, Long, Long) => Dataset[Row] = { (spark: SparkSession, config: Config, lowerBound: Long, upperBound: Long) =>

    val srcConnectionProps = new Properties()
    srcConnectionProps.put("user", config.getString("source.user"))
    srcConnectionProps.put("password", config.getString("source.password"))
    srcConnectionProps.put("driver", config.getString("source.driver"))

    val numPartitions = getNumPartitions(spark, config)

    log.info(s"""Setting the number of the partitions to $numPartitions based on the source table""")

    val jdbcDF = spark.read
      .option(JDBCOptions.JDBC_DRIVER_CLASS, config.getString("source.driver"))
      .option(JDBCOptions.JDBC_TABLE_NAME, config.getString("source.table"))
      .option(JDBCOptions.JDBC_BATCH_FETCH_SIZE, config.getInt("source.batchFetchSize"))
      .option(JDBCOptions.JDBC_PARTITION_COLUMN, config.getString("source.partitionColumn"))
      .option(JDBCOptions.JDBC_LOWER_BOUND, lowerBound)
      .option(JDBCOptions.JDBC_UPPER_BOUND, upperBound)
      .option(JDBCOptions.JDBC_NUM_PARTITIONS, numPartitions)
      .jdbc(config.getString("source.jdbcUrl"), config.getString("source.table"), srcConnectionProps)
      .where(s"""${config.getString("source.partitionColumn")} >= $lowerBound""")
    log.info(s"""Successfully Extracted data from table ${config.getString("source.table")} at ${config.getString("source.jdbcUrl")}""")
    jdbcDF
  }

  /**
    * Get the Max Value of a Column
    *
    * @return
    */
  def getMax: (SparkSession, String, String, String, Properties) => Long = { (spark: SparkSession, jdbcUrl: String, table: String, column: String, connectionProps: Properties) =>
    val sqlQuery = s"(SELECT MAX($column) AS max_$column FROM $table) as t"
    spark.sqlContext
      .read
      .jdbc(jdbcUrl, sqlQuery, connectionProps)
      .head()
      .getInt(0)
      .longValue()
  }

  /**
    * Get the Min Value of a Column
    *
    * @return
    */
  def getMin: (SparkSession, String, String, String, Properties) => Long = { (spark: SparkSession, jdbcUrl: String, table: String, column: String, connectionProps: Properties) =>
    val sqlQuery = s"(SELECT MIN($column) AS min_$column FROM $table) as t"
    spark.sqlContext
      .read
      .jdbc(jdbcUrl, sqlQuery, connectionProps)
      .head()
      .getInt(0)
      .longValue()
  }

  /**
    * Get the Row Count of a Table
    *
    * @return
    */
  def getRowCount: (SparkSession, String, String, String, Properties) => Long = { (spark: SparkSession, jdbcUrl: String, table: String, column: String, connectionProps: Properties) =>
    val sqlQuery = s"""(SELECT COUNT(CONCAT($column)) as rowcount from $table) as t"""
    spark.sqlContext
      .read
      .jdbc(jdbcUrl, sqlQuery, connectionProps)
      .head()
      .getLong(0)
      .longValue()
  }

  /**
    * Get the Number of Partitions based on the Partition Column
    *
    * @return
    */
  def getNumPartitions: (SparkSession, Config) => Long = { (spark: SparkSession, config: Config) =>
    val connectionProps = new Properties()
    connectionProps.put("user", config.getString("source.user"))
    connectionProps.put("password", config.getString("source.password"))
    connectionProps.put("driver", config.getString("source.driver"))

    val column = config.getString("source.partitionColumn")
    val table = config.getString("source.table")
    val jdbcUrl = config.getString("source.jdbcUrl")
    val (lowerBound, _) = getBounds(spark, config)

    val sqlQuery = s"(SELECT COUNT(distinct $column) AS num_partitions_$column FROM $table where $column >= $lowerBound) as t"
    spark.sqlContext
      .read
      .jdbc(jdbcUrl, sqlQuery, connectionProps)
      .head()
      .getLong(0)
      .longValue()
  }

  /**
    * Get the Lower and Upper Bounds for the data to be appended
    * @return
    */
  def getBounds: (SparkSession, Config) => (Long, Long) = { (spark: SparkSession, config: Config) =>

    val srcConnectionProps = new Properties()
    srcConnectionProps.put("user", config.getString("source.user"))
    srcConnectionProps.put("password", config.getString("source.password"))
    srcConnectionProps.put("driver", config.getString("source.driver"))

    val destConnectionProps = new Properties()
    destConnectionProps.put("user", config.getString("target.user"))
    destConnectionProps.put("password", config.getString("target.password"))
    destConnectionProps.put("driver", config.getString("target.driver"))

    config.getString("source.extractType") match {
      case "full" =>
        (getMin(spark, config.getString("source.jdbcUrl"), config.getString("source.table"), config.getString("source.partitionColumn"), srcConnectionProps),
          getMax(spark, config.getString("source.jdbcUrl"), config.getString("source.table"), config.getString("source.partitionColumn"), srcConnectionProps))
      case "incremental" =>
        (getMax(spark, config.getString("target.jdbcUrl"), config.getString("target.table"), config.getString("source.partitionColumn"), destConnectionProps) + 1,
          getMax(spark, config.getString("source.jdbcUrl"), config.getString("source.table"), config.getString("source.partitionColumn"), srcConnectionProps))
    }
  }


  /**
    * Verify the Row Counts of the Source and Target Tables
    * @return
    */
  def verifyRowCounts: (SparkSession, Config) => Unit = { (spark: SparkSession, config: Config) =>

    val srcConnectionProps = new Properties()
    srcConnectionProps.put("user", config.getString("source.user"))
    srcConnectionProps.put("password", config.getString("source.password"))
    srcConnectionProps.put("driver", config.getString("source.driver"))

    val destConnectionProps = new Properties()
    destConnectionProps.put("user", config.getString("target.user"))
    destConnectionProps.put("password", config.getString("target.password"))
    destConnectionProps.put("driver", config.getString("target.driver"))

    // Concat the Primary Columns into comma separated String for the Row Count
    val primaryColumns = config.getStringList("source.primaryColumns").asScala.toList.mkString(",")

    val srcRowCount = getRowCount(spark, config.getString("source.jdbcUrl"), config.getString("source.table"), primaryColumns, srcConnectionProps)
    val destRowCount = getRowCount(spark, config.getString("target.jdbcUrl"), config.getString("target.table"), primaryColumns, destConnectionProps)

    if (srcRowCount == destRowCount) {
      log.info(s"Source and Destination DB row counts match. Total Rows $destRowCount")
    } else {
      log.error(s"Destination Row Count $destRowCount does not match Source Row Count $srcRowCount")
    }
  }

  /**
    * Function to load data into JDBC datasource
    *
    * @return
    */
  def loadFn: (DataFrame, Config) => Unit = { (srcSqlDF: sql.DataFrame, config: Config) =>

    val destConnectionProps = new Properties()
    destConnectionProps.put("user", config.getString("target.user"))
    destConnectionProps.put("password", config.getString("target.password"))
    destConnectionProps.put("driver", config.getString("target.driver"))

    val load = Try(srcSqlDF
      .write
      .option(JDBCOptions.JDBC_DRIVER_CLASS, config.getString("target.driver"))
      .option(JDBCOptions.JDBC_TABLE_NAME, config.getString("target.table"))
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, config.getInt("target.batchInsertSize"))
      .mode(SaveMode.Append)
      .jdbc(config.getString("target.jdbcUrl"), config.getString("target.table"), destConnectionProps))
    if (load.isSuccess) {
      log.info(s"""Successfully Loaded ${srcSqlDF.count()} Rows into the table ${config.getString("target.table")} at ${config.getString("target.jdbcUrl")}""")
    } else {
      log.error(s"Exceptions encountered ${load.failed.get.getMessage}")
    }
  }

  /**
    * Save the Data as Parquet, Avro, Orc
    * @return
    */
  def saveAsFiles: (DataFrame, String, String) => Unit = { (srcSqlDF: sql.DataFrame,
                                                            directory: String, fileFormat: String) =>
    val saved = Try(srcSqlDF
      .write
      .format(s"$fileFormat")
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .save(directory))
    if (saved.isSuccess) {
      log.info(s"Successfully Saved the data as $fileFormat under $directory")
    } else {
      log.error(
        s"""Failed to save the data as $fileFormat.
           |Encountered Exception: ${saved.failed.get.getMessage}""".stripMargin)
    }
  }

  /**
    * Load the Data from Parquet, Avro, Orc Files into DB
    * @return
    */
  def loadFromFiles: (SparkSession, String, String) => sql.DataFrame = { (spark: SparkSession, directory: String, fileFormat: String) =>
    val loaded = Try(spark.read.format(fileFormat).option("compression", "snappy").load(directory))
    loaded.get
  }

  def main(args: Array[String]): Unit = {

    //Initialize Spark Session
    val sparkConf = new SparkConf()

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    // Load Config JSON File from the GCS Location provides as the first argument
    ConfigFactory.invalidateCaches()

    val configFile = args(0)

    val config = ConfigFactory
      .parseString(spark
        .read
        .json(configFile)
        .toJSON
        .collect()(0))

    config.resolve()

    // Load JDBC Drivers
    Class.forName(config.getString("source.driver")).newInstance
    if (config.getString("target.driver") != config.getString("source.driver")) {
      Class.forName(config.getString("target.driver")).newInstance
    }

    // Get Lower and Upper Bounds
    val (lowerBound, upperBound) = getBounds(spark, config)

    log.info(s"""${config.getString("source.extractType")} (Lower Bound, Upper Bound) for ${config.getString("source.partitionColumn")} : ($lowerBound, $upperBound)""")

    // Partials to reduce Arity
    def loadDBFn = loadFn(_: sql.DataFrame, config)
    def extractDBFn = extractFn(spark, _: Config, lowerBound, upperBound)

    if (lowerBound > upperBound) {

      log.warn(s"""Source DB may not have any updates since the last DB Sync.
           | Maximum Value for ${config.getString("source.partitionColumn")} is same between Source DB and Target DB""".stripMargin)
    } else {
      // Extract and Load Data from Source to Target
      loadDBFn(extractDBFn(config))
      verifyRowCounts(spark, config)
    }
    spark.stop()
  }
}
