package com.example.dataprocess

import kotlinx.coroutines.*
import java.util.*
import java.util.concurrent.Executors
import java.util.stream.Collectors
import kotlin.random.Random

class DataProcess {
	private val context = Executors.newFixedThreadPool(THREAD_POOL_SIZE).asCoroutineDispatcher()
	private val arrayListOfFulfillmentItems: List<FulfillmentItem> = (1..100000).map { FulfillmentItem(
		it,
		Random.nextInt(0, 100),
		Random.nextInt(1000, 9999).toString(),
		arrayOf("CD1", "CD2", "CD3", "CD4", "CD5").random(),
		Date())
	}

	companion object {
		const val THREAD_POOL_SIZE = 5 // externalizar
		const val IMS_SKU_LIST_MAX_SIZE = 250 // externalizar
	}

	fun process() = runBlocking {// fixed and dedicated thread pool
		arrayListOfFulfillmentItems
			.groupBy { it.shippingCenter }
			.entries
			.parallelStream()
			.forEach { itMap ->
				launch(context) {
//					println(Thread.currentThread().name + " >>> " + itMap.key)
					val scStock = getStockFromIMS(itMap.key, itMap.value.map { it.skuCode }.toSet())
					println(itMap.key + " - " + scStock.size + " skus")
					saveStock(itMap.key, scStock)
				}
			}
	}

//	fun process() = runBlocking {
//		arrayListOfFulfillmentItems
//			.groupBy { it.shippingCenter }
//			.entries
//			.parallelStream()
//			.map { itMap ->
//				val scStock = getStockFromIMS(itMap.key, itMap.value.map { it.skuCode }.toSet())
//				println(itMap.key + " - " + scStock.size + " skus")
//			}.flatMap(Collection<Stock>::stream)
//			.collect(Collectors.toList())
//	}

	private fun getStockFromIMS(cd: String, skuCodes: Set<String>): List<Stock> {
		println(Thread.currentThread().name + " >>> " + cd + " - " + skuCodes.size + " skus")
		val stock = skuCodes.chunked(IMS_SKU_LIST_MAX_SIZE).parallelStream().map {
			imsApi(cd, it.toSet())
		}.flatMap(Collection<Stock>::stream).collect(Collectors.toList())
		println(cd + stock)
		return stock
//		return skuCodes.chunked(IMS_SKU_LIST_MAX_SIZE)
//			.parallelStream()
//			.map { imsApi(cd, it.toSet()) }
//			.flatMap(Collection<Stock>::stream)
//			.collect(Collectors.toList())
	}

	private fun imsApi(cd: String, skuCodes: Set<String>): List<Stock> {
		runBlocking { delay(200) } // tempo médio de resposta
		val stock: MutableList<Stock> = mutableListOf()
		skuCodes.forEach { sku -> // mock retorno api
			stock.add(Stock(sku, Random.nextInt(0, 100)))
		}
		return stock
	}

	private fun saveStock(cd: String, stock: List<Stock>) {
		println("salvando estoque do $cd")
		runBlocking { delay(200) }
	}
}