package com.example.dataprocess

data class SimulationStock(
    val simulationId: Int,
    val shippingCenter: String,
    val skuCode: String,
    val availableQuantity: Int,
    val requestQuantity: Int?
)