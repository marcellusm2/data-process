package com.example.dataprocess

import kotlinx.coroutines.*
import java.util.*
import java.util.concurrent.Executors
import java.util.stream.Collectors
import kotlin.random.Random

class DataProcess {
	private val context = Executors.newFixedThreadPool(THREAD_POOL_SIZE).asCoroutineDispatcher()
	private val arrayListOfFulfillmentItems: List<FulfillmentItem> = (1..10000).map { FulfillmentItem(
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

	fun processWithFixedThreadPool() = runBlocking {
		arrayListOfFulfillmentItems
			.groupBy { it.shippingCenter }
			.entries
			.parallelStream()
			.forEach { itMap ->
				launch(context) {
//					println(Thread.currentThread().name + " >>> " + itMap.key)
					val scStock = getStockFromIMS(itMap.key, itMap.value.map { it.skuCode }.toSet()) // gerar record com sumarizado de skuCode e quantidade solicitada
					println(itMap.key + " - " + scStock.size + " skus")
					saveStock(scStock)
				}
			}
	}

	fun process() = runBlocking {
		val scStock = arrayListOfFulfillmentItems
			.groupBy { it.shippingCenter }
			.entries
			.stream()
			.map { itMap ->
				getStockFromIMS(itMap.key, itMap.value.map { it.skuCode }.toSet()) // gerar record com sumarizado de skuCode e quantidade solicitada
			}
			.flatMap { it.stream().map { item -> item } }
			.collect(Collectors.groupingBy(Stock::shippingCenter))

		arrayListOfFulfillmentItems
			.groupBy { it.shippingCenter }// agrupando por CD
			.entries
			.parallelStream()
			.map { itMap ->
				itMap.value.groupBy { it.skuCode } // agrupando SKUs de um CD
					.entries
					.parallelStream()
					.forEach {
						val stock = scStock[itMap.key]!!.find { sku -> sku.skuCode == it.key }
						println(
							itMap.key+" | SKU "+it.key+
							" -> estoque disponivel: "+stock!!.availableQuantity+
							" | qtd itens: "+it.value.size+
							" | qtd solictada: "+it.value.sumOf { item -> item.requestQuantity }+
							" >>> "+
							if (stock.availableQuantity <= 0) "ZERADO"
							else if (it.value.sumOf { item -> item.requestQuantity } < stock.availableQuantity) "SUFICIENTE"
							else ""
						)
					}
			}
			.collect(Collectors.toList())

		// agrupar items por sku sumarizando quantidade e com

//		scStock
//			.map {
//				SimulationStock(
//					1,
//					it.shippingCenter,
//					it.skuCode,
//					it.availableQuantity,
//					itMap.
//				)
//			}

//			.entries
//			.parallelStream()
//			.map { itMap ->
//				getStockFromIMS(itMap.key, itMap.value.map { it.skuCode }.toSet()) // gerar record com sumarizado de skuCode e quantidade solicitada
//			}
//			.collect(Collectors.toList())

//		println(itMap.key + " - " + scStock.size + " skus")
//		saveStock(itMap.key, scStock)

//		val aggregationByStateCity: Map<StateCityGroup, TaxEntryAggregation> = taxes.stream().collect(
//			groupingBy(
//				{ p -> StateCityGroup(p.getState(), p.getCity()) },
//				collectingAndThen(Collectors.toList(),
//					Function<R, RR> { list: R ->
//						val entries: Int = list.stream().collect(
//							summingInt(TaxEntrySimple::getNumEntries)
//						)
//						val priceAverage: Double = list.stream().collect(
//							averagingDouble(TaxEntrySimple::getPrice)
//						)
//						TaxEntryAggregation(entries, priceAverage)
//					})
//			)
//		)
	}

	private fun getStockFromIMS(cd: String, skuCodes: Set<String>): List<Stock> {
		println(Thread.currentThread().name + " >>> " + cd + " - " + skuCodes.size + " skus")
		val stock = skuCodes.chunked(IMS_SKU_LIST_MAX_SIZE)
			.parallelStream()
//			.stream()
			.map {
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
			stock.add(Stock(cd, sku, Random.nextInt(0, 100)))
		}
		return stock
	}

	private fun saveStock(stock: List<Stock>) {
		println("salvando estoque do ${stock.first().shippingCenter}")
		runBlocking { delay(200) }
	}

	private fun saveSimulationStock(stock: List<SimulationStock>) {
		println("salvando estoque do ${stock.first().shippingCenter} da simulação ${stock.first().simulationId}")
		runBlocking { delay(200) }
	}
}