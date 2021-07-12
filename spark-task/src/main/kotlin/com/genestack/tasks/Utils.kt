package com.genestack.tasks

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters
import scala.collection.Seq

fun SparkSession.loadFromDeltaLake(path: String): Dataset<Row> =
    read().format(DELTA_FORMAT).load(HDFS_PATH + path)

fun Dataset<*>.writeToDeltaLake(path: String) = write()
    .mode(SaveMode.Overwrite)
    .format(DELTA_FORMAT)
    .option(OVERWRITE_SCHEMA, true)
    .save(HDFS_PATH + path)

fun validateArgs(args: Array<String>, requiredLength: Int) =
    require(args.size >= requiredLength) { "Not enough arguments" }

fun <T> Collection<T>.toSeq(): Seq<T> = JavaConverters
    .collectionAsScalaIterableConverter(this)
    .asScala()
    .toSeq()
