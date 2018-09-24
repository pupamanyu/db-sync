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
name := "db-sync"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in Compile := Some("com.example.MySQLMigrate")

lazy val sparkVersion = "2.3.1"

lazy val mysqlVersion = "8.0.12"

lazy val configVersion = "1.3.2"

lazy val cloudSQLSocketFactoryVersion = "1.0.10"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "com.typesafe" % "config" % configVersion,
  //"com.google.cloud.sql" % "mysql-socket-factory-connector-j-8" % cloudSQLSocketFactoryVersion
)
resolvers += Resolver.mavenLocal
