package com.example.dataprocess

data class Stock(
    val shippingCenter: String,
    val skuCode: String,
    val availableQuantity: Int
)

data class StockSimulation(
    val shippingCenter: String,
    val skuCode: String,
    val requestQuantity: Int,
    val availableQuantity: Int? = null
)
