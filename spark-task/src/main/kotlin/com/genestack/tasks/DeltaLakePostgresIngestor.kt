package com.genestack.tasks

import org.apache.spark.sql.SparkSession
import java.util.*

fun main(args: Array<String>) {
    validateArgs(args, requiredLength = 7)

    val host = args[0]
    val port = args[1]
    val database = args[2]
    val table = args[3]
    val user = args[4]
    val password = args[5]
    val outputFile = args[6]

    val jdbcUrl = "jdbc:postgresql://$host:$port/$database"
    val jdbcConfiguration = Properties().apply {
        put("user", user)
        put("password", password)
        put("driver", "org.postgresql.Driver")
    }

    val sparkSession = SparkSession.builder()
        .appName("ingest_postgres_table_to_delta_lake")
        .orCreate

    sparkSession
        .read()
        .jdbc(jdbcUrl, table, jdbcConfiguration)
        .writeToDeltaLake(outputFile)
}
