package com.genestack

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.*
import java.nio.file.Path
import kotlin.io.path.exists

/**
 * Basic examples how to work with delta lake
 */
class DeltaLakeTest {

    private lateinit var sparkSession: SparkSession

    companion object {
        private const val STUDENTS_FILE_PATH = "data/students.csv"

        const val TABLE = "delta-lake/samples-table"
        const val DELTA = "delta"
        val SCHEMA = buildString {
            repeat(5) { append("col$it INT,") }
            for (i in 5..19) {
                append("col$i STRING,")
            }
            append("col20 STRING")
        }

        @JvmStatic
        @BeforeAll
        fun generateSamplesFilesIfNotExist() = runBlocking {
            if (Path.of(SAMPLES_FILE_PATH).exists()) {
                println("File $SAMPLES_FILE_PATH exists, skipping")
                return@runBlocking
            }

            launch(Dispatchers.IO) {
                generateCsv(defaultCsvGenerator())
            }

            launch(Dispatchers.IO) {
                generateCsv(defaultCsvGenerator()) {
                    path = SAMPLES_WITH_EXTRA_ROWS_PATH
                    rowNumber = 1_100_000
                }
            }

            launch(Dispatchers.IO) {
                generateCsv(defaultCsvGenerator()) {
                    path = SAMPLES_WITH_CHANGED_COLUMN_PATH
                    column {
                        name = "integer_col_0"

                        var value = 0
                        nextValue = { value++ }
                    }
                }
            }
        }

        @JvmStatic
        @BeforeAll
        fun generateStudentsFileIfNotExist() {
            val file = File(STUDENTS_FILE_PATH)
            if (file.exists()) {
                println("File $STUDENTS_FILE_PATH exists, skipping")
                return
            }

            file.writeText(
                """
                1,John Cena,44
                2,Chuck Norris,81
                3,Arnold Schwarzenegger,73
                """.trimIndent()
            )
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
        listOf("delta-lake", "metastore_db", "spark-warehouse")
            .map { Path.of(it).toFile() }
            .forEach { it.deleteRecursively() }

        // remove samples files manually if necessary
    }

    @Test
    fun `insert once`() {
        sparkSession.read()
            .withHeader()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(DELTA)
            .save(TABLE)

        assertEquals(ROWS_NUMBER.toLong(), getCount())
    }

    @Test
    fun `insert several times`() = repeat(10) {
        sparkSession.read()
            .withHeader()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(DELTA)
            .mode(SaveMode.Overwrite)
            .save(TABLE)

        assertEquals(ROWS_NUMBER.toLong(), getCount())
    }

    @Test
    fun `insert new rows`() {
        sparkSession.read()
            .withHeader()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(DELTA)
            .save(TABLE)

        sparkSession
            .read()
            .withHeader()
            .csv(SAMPLES_WITH_EXTRA_ROWS_PATH)
            .write()
            .format(DELTA)
            .mode(SaveMode.Overwrite)
            .save(TABLE)

        assertEquals(1_100_000L, getCount())
    }

    @Test
    fun `update column and read several versions`() {
        sparkSession.read()
            .withHeader()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(DELTA)
            .save(TABLE)

        sparkSession
            .read()
            .withHeader()
            .csv(SAMPLES_WITH_CHANGED_COLUMN_PATH)
            .write()
            .format(DELTA)
            .mode(SaveMode.Overwrite)
            .save(TABLE)

        assertEquals(ROWS_NUMBER.toLong(), getCount())
        sparkSession.read()
            .format(DELTA)
            .load(TABLE)
            .select("integer_col_0")
            .show(10)

        sparkSession.read()
            .format(DELTA)
            .option("versionAsOf", 0)
            .load(TABLE)
            .select("integer_col_0")
            .show(10)
    }

    // TODO figure out why it fails with oom when using schema
    @Test
    fun `insert once with schema`() {
        sparkSession.read()
            .withHeader()
            .schema(SCHEMA)
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(DELTA)
            .save(TABLE)

        assertEquals(ROWS_NUMBER.toLong(), getCount())
    }

    // TODO figure out why it fails with oom when using schema
    @Test
    fun `insert untyped`() {
        sparkSession.read()
            .withHeader()
            .schema(SCHEMA)
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(DELTA)
            .save(TABLE)

        sparkSession.read()
            .withHeader()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format(DELTA)
            .mode(SaveMode.Overwrite)
            .option("overwriteSchema", "true")
            .save(TABLE)
    }

    @Test
    fun `insert as json`() {
        sparkSession.read()
            .withHeader()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format("json")
            .mode(SaveMode.Overwrite)
            .save("delta-lake/samples-json")
    }

    @Test
    fun `insert as parquet`() {
        sparkSession.read()
            .withHeader()
            .csv(SAMPLES_FILE_PATH)
            .write()
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save("delta-lake/samples-parquet")
    }

    @Test
    fun `select from delta table stored in path`() {
        sparkSession.read()
            .schema("id INT, name STRING, age INT")
            .csv(STUDENTS_FILE_PATH)
            .write()
            .format(DELTA)
            .mode(SaveMode.Overwrite)
            .save("delta-lake/students-table")

        sparkSession
            .sql("SELECT id, name, age from delta.`/Users/anatoly.nikiforov/workspace/DataLakePoC/delta-lake/students-table`;")
            .show()
    }

    @Test
    fun `select from delta table stored in metastore`() {
        sparkSession.read()
            .schema("id INT, name STRING, age INT")
            .csv(STUDENTS_FILE_PATH)
            .write()
            .mode(SaveMode.Overwrite)
            .saveAsTable("students")

        sparkSession.sql("SELECT id, name, age from students;").show()
    }

    private fun getCount() = sparkSession
        .read()
        .format(DELTA)
        .load(TABLE)
        .count()

    private fun DataFrameReader.withHeader() = option("header", true)
}
