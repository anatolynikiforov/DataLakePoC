package com.genestack

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.RandomStringUtils
import java.io.*
import kotlin.random.Random

const val SAMPLES_FILE_PATH = "data/samples.csv"
const val SAMPLES_WITH_EXTRA_ROWS_PATH = "data/samples_with_extra_rows.csv"
const val SAMPLES_WITH_CHANGED_COLUMN_PATH = "data/samples_with_changed_column.csv"

const val ROWS_NUMBER = 1_000_000

private const val INTEGER_COLUMNS_NUMBER = 5
private const val ENUM_COLUMNS_NUMBER = 10
private const val STRING_COLUMNS_NUMBER = 5

private const val ENUM_SIZE = 1000
private const val STRING_LENGTH = 20

fun main(): Unit = runBlocking {
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

fun generateCsv(
    generator: CsvGenerator = CsvGenerator(),
    setup: CsvGenerator.() -> Unit = {}
) = generator.apply(setup).generate()

fun defaultCsvGenerator() = csvGenerator {
    path = SAMPLES_FILE_PATH
    rowNumber = ROWS_NUMBER

    val random = Random(42)

    repeat(INTEGER_COLUMNS_NUMBER) {
        column {
            name = "integer_col_$it"
            nextValue = { random.nextInt() }
        }
    }

    for (enumPrefix in 'A'..'A' + ENUM_COLUMNS_NUMBER) {
        column {
            name = "enum_col_${enumPrefix - 'A'}"
            nextValue = { enumPrefix.toString() + random.nextInt(ENUM_SIZE) }
        }
    }

    repeat(STRING_COLUMNS_NUMBER) {
        column {
            name = "string_col_$it"
            nextValue = { RandomStringUtils.randomAlphanumeric(STRING_LENGTH) }
        }
    }
}

fun csvGenerator(setup: CsvGenerator.() -> Unit) = CsvGenerator().apply(setup)

class CsvGenerator {
    lateinit var path: String
    var rowNumber: Int = 0
    private val columns = mutableListOf<CsvColumn>()

    fun column(setup: CsvColumn.() -> Unit) {
        val column = CsvColumn().apply(setup)
        val oldColumn = columns.find { it.name == column.name }

        if (oldColumn == null) {
            columns.add(column)
        } else {
            columns[columns.indexOf(oldColumn)] = column
        }
    }

    fun generate() {
        BufferedWriter(FileWriter(path)).use { writer ->
            val header = buildHeader()
            writer.write(header)

            repeat(rowNumber) {
                val row = buildRow()
                writer.write(row)
            }
        }
    }

    private fun buildHeader() = buildRow { name }

    private fun buildRow(
        value: CsvColumn.() -> Any = { nextValue() }
    ) = buildString {
        for ((index, column) in columns.withIndex()) {
            append(column.value())
            if (index < columns.size - 1) {
                append(",")
            }
        }

        append("\n")
    }
}

class CsvColumn {
    lateinit var name: String
    lateinit var nextValue: () -> Any
}
