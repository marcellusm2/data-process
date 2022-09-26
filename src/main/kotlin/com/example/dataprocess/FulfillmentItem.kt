package com.example.dataprocess

import java.util.*

data class FulfillmentItem(
    val id: Int,
    val requestQuantity: Int,
    val skuCode: String,
    val shippingCenter: String,
    val createdAt: Date
)
