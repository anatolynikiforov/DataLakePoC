package com.genestack.tasks

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

private const val FEATURES_COLUMN = "features"
private const val PREDICTION_COLUMN = "prediction"

fun main() {
    val sparkSession = SparkSession.builder()
        .appName("covid_data_curation")
        .orCreate

    val covidData = sparkSession.loadFromDeltaLake("covid-denormalized-data")
        .na()
        .fill(0) // replace null values with 0

    val assembler = VectorAssembler().apply {
        inputCols = arrayOf(
            "new_cases",
            "new_deaths",
            "new_vaccinations",
            "people_fully_vaccinated",
            "people_vaccinated",
            "total_cases",
            "total_deaths",
            "total_vaccinations"
        )
        outputCol = FEATURES_COLUMN
    }

    val regression = KMeans().apply {
        featuresCol = FEATURES_COLUMN
        maxIter = 200
        predictionCol = PREDICTION_COLUMN
    }

    val mlPipeline = Pipeline().apply {
        stages = arrayOf(assembler, regression)
    }

    val model = mlPipeline.fit(covidData)
    val resultDf = model.transform(covidData)
        .cache()

    val resultColumns = resultDf.columns()
        .filterNot { it == FEATURES_COLUMN || it == PREDICTION_COLUMN }
        .map { col(it) }
        .toTypedArray()

    resultDf
        .select(*resultColumns)
        .where(col(PREDICTION_COLUMN).equalTo(0))
        .writeToDeltaLake("covid-curated-data")

    resultDf
        .select(*resultColumns)
        .where(col(PREDICTION_COLUMN).equalTo(1))
        .writeToDeltaLake("covid-anomaly-data")
}
