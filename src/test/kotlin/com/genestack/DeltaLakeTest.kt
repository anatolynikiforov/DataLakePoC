package com.genestack

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Basic examples how to work with delta lake
 */
class DeltaLakeTest {

    private lateinit var sparkSession: SparkSession

    companion object {
        const val TABLE = "delta-lake/samples-table"
        const val FORMAT = "delta"
        const val MODE = "overwrite"
        val SCHEMA = buildString {
            repeat(5) { append("col$it INT,") }
            for (i in 5..19) {
                append("col$i STRING,")
            }
            append("col20 STRING")
        }
    }

    @BeforeEach
    fun setUp() {
        sparkSession = SparkSession.builder()
            .appName("DeltaLake")
            .master("local[*]")
            .enableHiveSupport()
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .orCreate
    }

    @AfterEach
    fun tearDown() {
        // todo remove created files
    }

    @Test
    fun `insert once`() {
        sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(FORMAT)
            .save(TABLE)

        assertEquals(ROWS_NUMBER.toLong(), getCount())
    }

    @Test
    fun `insert several times`() = repeat(10) {
        sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(FORMAT)
            .mode(MODE)
            .save(TABLE)

        assertEquals(ROWS_NUMBER.toLong(), getCount())
    }

    @Test
    fun `insert new rows`() {
        sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(FORMAT)
            .save(TABLE)

        sparkSession
            .read()
            .csv(SAMPLES_WITH_EXTRA_ROWS_PATH)
            .write()
            .format(FORMAT)
            .mode(MODE)
            .save(TABLE)

        assertEquals(1_100_001L, getCount())
    }

    @Test
    fun `update column and read several versions`() {
        sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
            .write()
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

        sparkSession.read()
            .format(FORMAT)
            .option("versionAsOf", 0)
            .load(TABLE)
            .select("_c0")
            .show(10)
    }

    @Test
    fun `insert once with schema`() {
        sparkSession.read()
            .schema(SCHEMA)
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(FORMAT)
            .save(TABLE)

        assertEquals(ROWS_NUMBER + 1L, getCount())
    }

    @Test
    fun `insert untyped`() {
        sparkSession.read()
            .schema(SCHEMA)
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(FORMAT)
            .save(TABLE)

        sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(FORMAT)
            .mode(MODE)
            .save(TABLE)
    }

    @Test
    fun `insert as json`() {
        sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format("json")
            .mode(MODE)
            .save("delta-lake/samples-json")
    }

    @Test
    fun `insert as parquet`() {
        sparkSession.read()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format("parquet")
            .mode(MODE)
            .save("delta-lake/samples-parquet")
    }


    @Test
    fun `select from delta table stored in path`() {
        sparkSession.read()
            .schema("id INT, name STRING, age INT")
            .csv("data/students.csv")
            .write()
            .format(FORMAT)
            .mode(MODE)
            .save("delta-lake/students-table")

        sparkSession
            .sql("SELECT id, name, age from delta.`/Users/anatoly.nikiforov/workspace/DataLakePoC/delta-lake/students-table`;")
            .show()
    }

    @Test
    fun `select from delta table stored in metastore`() {
        sparkSession.read()
            .schema("id INT, name STRING, age INT")
            .csv("data/students.csv")
            .write()
            .mode(MODE)
            .saveAsTable("students")

        sparkSession.sql("SELECT id, name, age from students;").show()
    }

    private fun getCount() = sparkSession
        .read()
        .format(FORMAT)
        .load(TABLE)
        .count()
}
