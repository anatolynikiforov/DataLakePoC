package com.genestack.tasks

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

fun main(args: Array<String>) {
    validateArgs(args, requiredLength = 6)

    val host = args[0]
    val port = args[1]
    val database = args[2]
    val collection = args[3]
    val user = args[4]
    val password = args[5]

    val uri = "mongodb://$user:$password@$host:$port/$database.$collection?authSource=admin"

    val sparkSession = SparkSession.builder()
        .appName("covid_data_mongo_export")
        .config("spark.mongodb.input.uri", uri)
        .config("spark.mongodb.output.uri", uri)
        .orCreate

    val covidData = sparkSession.loadFromDeltaLake("covid-curated-data")
    MongoSpark.save(covidData)
}
