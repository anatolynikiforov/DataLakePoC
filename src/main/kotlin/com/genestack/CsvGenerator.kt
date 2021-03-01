package com.genestack

import java.io.*
import java.lang.StringBuilder
import java.util.*
import kotlin.random.Random

const val ROWS_NUMBER = 1_000_000

const val INTEGER_COLUMNS_NUMBER = 5
const val ENUM_COLUMNS_NUMBER = 10
const val STRING_COLUMNS_NUMBER = 5

const val ENUM_SIZE = 1000
const val STRING_LENGTH = 20

fun main() {
    val random = Random(42)
    BufferedWriter(FileWriter("data/samples.csv")).use { writer ->
        repeat(ROWS_NUMBER) {
            val builder = StringBuilder()
            repeat(INTEGER_COLUMNS_NUMBER) {
                builder.append(random.nextInt()).appendComma()
            }

            for (enum in 'A'..'A' + ENUM_COLUMNS_NUMBER) {
                builder.append(enum.toString() + random.nextInt(ENUM_SIZE)).appendComma()
            }

            repeat(STRING_COLUMNS_NUMBER) {
                builder.append(UUID.randomUUID().toString().take(STRING_LENGTH))
                if (it < STRING_COLUMNS_NUMBER - 1) {
                    builder.appendComma()
                }
            }

            builder.append("\n")
            writer.write(builder.toString())
        }
    }
}

private fun StringBuilder.appendComma() = append(",")
