package com.genestack.tasks

import org.apache.spark.sql.SparkSession

fun main(args: Array<String>) {
    validateArgs(args, requiredLength = 3)

    val (inputFile, inputFileFormat, outputFile) = args.toList()

    val sparkSession = SparkSession.builder()
        .appName("ingest_file_to_delta_lake")
        .orCreate

    sparkSession.read()
        .format(inputFileFormat)
        .apply {
            if (inputFileFormat == CSV_FORMAT) {
                option("header", true)
                option("inferSchema", true)
            }
        }

        .load(inputFile)
        .writeToDeltaLake(outputFile)
}
