package com.genestack.tasks

import org.apache.spark.sql.SparkSession

private const val ID_FIELD = "iso_code"

fun main() {
    val sparkSession = SparkSession.builder()
        .appName("covid_data_denormalization")
        .orCreate

    val countryInfo = sparkSession.loadFromDeltaLake("country-info")
    val covidDeaths = sparkSession.loadFromDeltaLake("covid-deaths")
    val vaccinations = sparkSession.loadFromDeltaLake("covid_vaccinations")

    countryInfo
        .join(covidDeaths, ID_FIELD)
        .join(vaccinations, listOf(ID_FIELD, "date").toSeq())
        .writeToDeltaLake("covid-denormalized-data")
}
