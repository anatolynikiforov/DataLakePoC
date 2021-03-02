package com.genestack

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DeltaLakeTest {

    private lateinit var sparkSession: SparkSession
    private lateinit var data: Dataset<Row>

    companion object {
        const val TABLE = "delta-lake/samples-table"
        const val FORMAT = "delta"
        const val MODE = "overwrite"
    }

    @BeforeEach
    fun setUp() {
        sparkSession = SparkSession.builder()
            .appName("DeltaLake")
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .orCreate

        data = sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
    }

    @AfterEach
    fun tearDown() {
        // todo remove created files
    }

    @Test
    fun `insert once`() {
        data.write()
            .format(FORMAT)
            .save(TABLE)

        assertEquals(ROWS_NUMBER + 1, getCount())
    }

    @Test
    fun `insert several times`() = repeat(10) {
        data.write()
            .format(FORMAT)
            .mode(MODE)
            .save(TABLE)

        assertEquals(ROWS_NUMBER + 1, getCount())
    }

    @Test
    fun `insert new rows`() {
        data.write()
            .format(FORMAT)
            .save(TABLE)

        sparkSession
            .read()
            .csv(SAMPLES_WITH_EXTRA_ROWS_PATH)
            .write()
            .format(FORMAT)
            .mode(MODE)
            .save(TABLE)

        assertEquals(1_100_001, getCount())
    }

    @Test
    fun `update column`() {
        data.write()
            .format(FORMAT)
            .save(TABLE)

        sparkSession
            .read()
            .csv(SAMPLES_WITH_CHANGED_COLUMN_PATH)
            .write()
            .format(FORMAT)
            .mode(MODE)
            .save(TABLE)

        assertEquals(ROWS_NUMBER + 1L, getCount())
        sparkSession.read()
            .format(FORMAT)
            .load(TABLE)
            .select("_c0") // todo
            .show(10)
    }

    private fun getCount() = sparkSession
        .read()
        .format(FORMAT)
        .load(TABLE)
        .count()
}
