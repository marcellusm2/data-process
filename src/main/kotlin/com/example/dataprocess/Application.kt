package com.example.dataprocess

import kotlin.system.measureTimeMillis

class Application

fun main(args: Array<String>) {
	val time = measureTimeMillis {
		DataProcess().process()
	}
	println("$time ms")
}
