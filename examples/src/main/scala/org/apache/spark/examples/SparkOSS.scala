/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import io.delta.tables._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object SparkOSS {
    def main(args: Array[String]): Unit = {
        // ossDemo()
        deltaDemo()
    }

    def deltaDemo(): Unit = {
        val spark = SparkSession
          .builder
          .appName("SparkDelta")
          .master("local")
          .config("spark.delta.logStore.class",
              "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
          .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl",
              "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
          .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
          .config("spark.hadoop.fs.s3a.endpoint", "127.0.0.1:9000")
          .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
          .getOrCreate()
        val path = "s3a://bucket1/delta-table" // or local file "./delta-table"
        val deltaTable = DeltaTable.forPath(spark, path)
        val fullHistoryDF = deltaTable.history()    // get the full history of the table
        fullHistoryDF.foreach(println(_))
        // val data = spark.range(0, 5)
        // data.write.format("delta").mode("overwrite").save(path)
    }

    def ossDemo(): Unit = {
        val sparkConf = new SparkConf().setAppName("SparkOSS").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        sc.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin")
        sc.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "127.0.0.1:9000")
        sc.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
        // val ds = sc.range(0, 5)
        // ds.write.format("delta").mode("overwrite").save("s3a://bucket1/delta-table")
        val rdd = sc.textFile("s3a://bucket1/README.md")
        rdd.foreach { line =>
            val text = "OSS:" + line
            println(text)
        }
        sc.stop()
    }
}
// scalastyle:on println
