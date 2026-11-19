package com.example.service

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.os.BatteryManager
import com.example.data.NetworkTransport
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

data class DeviceTelemetry(
    val networkTransport: NetworkTransport = NetworkTransport.OFFLINE,
    val batteryPercent: Int = 100,
    val signalBars: Int = 0
)

class NetworkAndBatteryMonitor(context: Context) {
    private val appContext = context.applicationContext
    private val connectivityManager =
        appContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager

    private val _telemetry = MutableStateFlow(readInitialTelemetry())
    val telemetry: StateFlow<DeviceTelemetry> = _telemetry.asStateFlow()

    private var registered = false

    private val networkCallback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) = refreshNetwork()
        override fun onLost(network: Network) = refreshNetwork()
        override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) {
            refreshNetwork()
        }
    }

    private val batteryReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent == null) return
            val level = intent.getIntExtra(BatteryManager.EXTRA_LEVEL, -1)
            val scale = intent.getIntExtra(BatteryManager.EXTRA_SCALE, 100).coerceAtLeast(1)
            if (level >= 0) {
                val pct = ((level * 100f) / scale).toInt().coerceIn(0, 100)
                _telemetry.value = _telemetry.value.copy(batteryPercent = pct)
            }
        }
    }

    fun start() {
        if (registered) return
        registered = true
        refreshNetwork()
        runCatching {
            connectivityManager.registerDefaultNetworkCallback(networkCallback)
        }
        runCatching {
            val sticky = appContext.registerReceiver(
                batteryReceiver,
                IntentFilter(Intent.ACTION_BATTERY_CHANGED)
            )
            if (sticky != null) {
                batteryReceiver.onReceive(appContext, sticky)
            }
        }
    }

    fun stop() {
        if (!registered) return
        registered = false
        runCatching { connectivityManager.unregisterNetworkCallback(networkCallback) }
        runCatching { appContext.unregisterReceiver(batteryReceiver) }
    }

    private fun readInitialTelemetry(): DeviceTelemetry {
        val (transport, bars) = inspectActiveNetwork()
        val bm = appContext.getSystemService(Context.BATTERY_SERVICE) as? BatteryManager
        val pct = bm?.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY)
            ?.takeIf { it in 0..100 } ?: 100
        return DeviceTelemetry(
            networkTransport = transport,
            batteryPercent = pct,
            signalBars = bars
        )
    }

    private fun refreshNetwork() {
        val (transport, bars) = inspectActiveNetwork()
        _telemetry.value = _telemetry.value.copy(
            networkTransport = transport,
            signalBars = bars
        )
    }

    private fun inspectActiveNetwork(): Pair<NetworkTransport, Int> {
        val activeNetwork = connectivityManager.activeNetwork ?: return NetworkTransport.OFFLINE to 0
        val caps = connectivityManager.getNetworkCapabilities(activeNetwork)
            ?: return NetworkTransport.OFFLINE to 0
        val hasInternet = caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) &&
            caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)
        if (!hasInternet && !caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)) {
            return NetworkTransport.OFFLINE to 0
        }
        return when {
            caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> NetworkTransport.WIFI to 4
            caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> NetworkTransport.CELLULAR to 3
            caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> NetworkTransport.WIFI to 4
            else -> NetworkTransport.OFFLINE to 0
        }
    }
}
