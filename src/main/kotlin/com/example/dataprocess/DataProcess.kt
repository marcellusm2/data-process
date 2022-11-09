package com.example.dataprocess

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.merge
import java.util.*
import java.util.concurrent.Executors
import java.util.stream.Collectors
import kotlin.random.Random

class DataProcess {
	private val context = Executors.newFixedThreadPool(THREAD_POOL_SIZE).asCoroutineDispatcher()
	private val arrayListOfFulfillmentItems: List<FulfillmentItem> = (1..10000).map { FulfillmentItem(
		it,
		Random.nextInt(0, 100),
		Random.nextInt(1000, 1005).toString(),
		arrayOf("CD1", "CD2", "CD3", "CD4", "CD5").random(),
		Date())
	}

	private val arrayListOfFulfillmentItems2: List<FulfillmentItem> = (1..10000).map { FulfillmentItem(
		it,
		Random.nextInt(0, 100),
		Random.nextInt(2000, 20000).toString(),
		arrayOf("CD1", "CD2", "CD3", "CD4", "CD5").random(),
		Date())
	}

	companion object {
		const val THREAD_POOL_SIZE = 5 // externalizar
		const val IMS_SKU_LIST_MAX_SIZE = 200 // externalizar
	}

	fun processWithFixedThreadPool() = runBlocking {

		val list = mutableListOf<FulfillmentItem>()
		arrayListOfFulfillmentItems.forEach{list.add(it)}
		arrayListOfFulfillmentItems2.forEach{list.add(it)}

		list
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

//		arrayListOfFulfillmentItems
//			.groupBy { it.shippingCenter }// agrupando por CD
//			.entries
//			.stream()
//			.forEach { items ->
//				val itemsWithTheSameSku = items.value.groupBy { it.skuCode }
////				itemsWithTheSameSku.entries.forEach { println(items.key + "|" + it.key + " - " + it.value.size) }
////				enviarPacotes(itemsWithTheSameSku)
//				saveStockSimulation(itemsWithTheSameSku, items.key)
//			}


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

//	fun <T> buffer(stream: Stream<T>, count: Long): Stream<List<T>?>? {
//		val streamIterator: Iterator<T> = stream.iterator()
//		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(object : Iterator<List<T>?> {
//			override fun hasNext(): Boolean {
//				return streamIterator.hasNext()
//			}
//
//			override fun next(): List<T> {
//				if (!hasNext()) {
//					throw NoSuchElementException()
//				}
//				val intermediate: MutableList<T> = ArrayList()
//				var v: Long = 0
//				while (v < count && hasNext()) {
//					intermediate.add(streamIterator.next())
//					v++
//				}
//				return intermediate
//			}
//		}, 0), false)
//	}

	private fun enviarPacotes(list: Map<String, List<FulfillmentItem>>) {
		val pacotes = mutableListOf<List<FulfillmentItem>>(mutableListOf())
		var pacotePosition = 0

		list.forEach {
			if (it.value.size >= IMS_SKU_LIST_MAX_SIZE) {
				pacotes.add(it.value)
				pacotePosition++
			} else {
				if (pacotes[pacotePosition].size < IMS_SKU_LIST_MAX_SIZE) {
					pacotes[pacotePosition] += it.value
				} else {
					pacotes.add(it.value)
					pacotePosition++
				}
			}
		}

		var count = 1
		pacotes.forEach {
			println("enviando pacote "+count+"/"+pacotes.size+" com "+it.size+" itens")
			count++
		}
	}

	private fun saveStockSimulation(listCD: Map<String, List<FulfillmentItem>>, cd: String) {
		listCD.forEach{ skuCd ->
			var qty = 0
			skuCd.value.forEach {
				qty += it.requestQuantity
			}
//			stockSimulationService.save(
//				StockSimulation(
//					id = null,
//					simulation = simulation,
//					shippingCenter = skuCd.value[0].shippingCenter!!,
//					skuCode = skuCd.key,
//					availableStock = null,
//					requestedStock = qty,
//					lastSync = null
//				)
//			)
		}
		listCD.forEach { sku ->
			println(cd + "|" + sku.key + " - " + sku.value.sumOf { it.requestQuantity })
		}
		listCD.entries.parallelStream().forEach { sku ->
			println(sku.value.first().shippingCenter + "|" + sku.key + " - " + sku.value.sumOf { it.requestQuantity })
		}
	}

	fun process2() = runBlocking {

		val list = mutableListOf<FulfillmentItem>()
		arrayListOfFulfillmentItems.forEach{list.add(it)}
		arrayListOfFulfillmentItems2.forEach{list.add(it)}

		list
			.groupBy { it.shippingCenter }// agrupando por CD
			.entries
			.parallelStream()
			.map { itMap -> //aqui é item de uma lista de sku por cd
				enviarItensAgrupadosPorSku(itMap.value.groupBy { it.skuCode }) // agrupando SKUs de um CD
			}
			.collect(Collectors.toList())
	}

	private fun enviarItensAgrupadosPorSku(list: Map<String, List<FulfillmentItem>>) {

		val pacotes = mutableListOf<MutableList<FulfillmentItem>>()
		var pacotePosition = 0

		list.forEach{
			if (it.value.size >= 200){
				pacotes.add(it.value.toMutableList())
				pacotePosition += 1
			} else {
				if (pacotes.isEmpty()){
					pacotes.add(mutableListOf())
				}
				if (pacotes[pacotePosition].size < 200){
					pacotes[pacotePosition]+= it.value
				} else {
					pacotes.add(it.value.toMutableList())
					pacotePosition += 1
				}
			}
		}

		println("ok")

	}
}