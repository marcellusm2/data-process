package com.example.dataprocess

import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import kotlin.streams.toList
import kotlin.system.measureTimeMillis


class Application

fun main() {
//	var time = measureTimeMillis {
//		DataProcess().processWithFixedThreadPool()
//	}
//	println("fixedThreadPool: $time ms")

	val time = measureTimeMillis {
		DataProcess().process()
	}
	println("$time ms")


//	val localDate1: LocalDate = date1.toInstant()
//		.atZone(ZoneId.systemDefault())
//		.toLocalDate()
//	val localDate2: LocalDate = date2.toInstant()
//		.atZone(ZoneId.systemDefault())
//		.toLocalDate()
}
