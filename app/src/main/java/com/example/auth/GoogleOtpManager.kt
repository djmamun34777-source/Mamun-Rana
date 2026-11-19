package com.example.auth

import android.content.Context
import android.util.Log
import com.example.BuildConfig
import com.example.R
import com.example.data.DeviceIdentityStore
import com.example.data.OperationType
import com.example.data.handleFirestoreError
import com.google.firebase.auth.FirebaseUser
import com.google.firebase.firestore.FieldValue
import com.google.firebase.firestore.FirebaseFirestore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.tasks.await
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.concurrent.TimeUnit

private const val OTP_TAG = "GoogleOtpManager"
private const val OTP_COLLECTION = "otp_verifications"
private const val OTP_VALIDITY_MS = 10 * 60 * 1000L // 10 minutes
private const val MAX_OTP_ATTEMPTS = 5
private const val PLACEHOLDER_RESEND_KEY = "MY_RESEND_API_KEY"

sealed interface OtpSendResult {
    data class Sent(val email: String) : OtpSendResult
    data class EmailServiceNotConfigured(val email: String) : OtpSendResult
    data class Error(val message: String) : OtpSendResult
}

sealed interface OtpVerifyResult {
    data object Verified : OtpVerifyResult
    data class InvalidCode(val remainingAttempts: Int) : OtpVerifyResult
    data object Expired : OtpVerifyResult
    data object TooManyAttempts : OtpVerifyResult
    data class Error(val message: String) : OtpVerifyResult
}

/**
 * Manages real 6-digit Email OTP verification sent to the signed-in Google account's Gmail inbox.
 *
 * Security design:
 * - Generates a cryptographically secure 6-digit OTP via [SecureRandom].
 * - Never stores plaintext OTPs in Firestore; only a 64-char SHA-256 hash (`uid:deviceId:otp`) is saved
 *   under `/otp_verifications/{uid}` protected by self-only Firestore rules.
 * - Sends real emails via Resend HTTPS API using `BuildConfig.RESEND_API_KEY` (configured via AI Studio Secrets).
 */
class GoogleOtpManager(context: Context) {

    private val appContext = context.applicationContext
    private val identityStore = DeviceIdentityStore(appContext)
    private val db: FirebaseFirestore = FirebaseFirestore.getInstance(
        appContext.getString(R.string.firestore_database_id)
    )
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .writeTimeout(15, TimeUnit.SECONDS)
        .build()

    /**
     * Returns true ONLY if this Google UID + local Device ID has already completed OTP verification.
     */
    suspend fun isSessionOtpVerified(user: FirebaseUser): Boolean {
        val uid = user.uid
        if (uid.isBlank()) return false
        val prefs = identityStore.readSnapshot()
        val localDeviceId = identityStore.getOrCreateDeviceId()

        // Fast path: local verified marker matches current Google account
        if (prefs.verifiedOtpUid == uid && prefs.verifiedOtpEmail.equals(user.email.orEmpty(), ignoreCase = true)) {
            return true
        }

        // Cross-check Firestore record for this UID + same Device ID
        val docPath = "$OTP_COLLECTION/$uid"
        return try {
            val snap = db.collection(OTP_COLLECTION).document(uid).get().await()
            val verified = snap.getBoolean("verified") == true
            val storedDeviceId = snap.getString("deviceId").orEmpty()
            val storedEmail = snap.getString("email").orEmpty()
            if (verified && storedDeviceId == localDeviceId && storedEmail.equals(user.email.orEmpty(), ignoreCase = true)) {
                identityStore.markOtpVerified(uid, storedEmail)
                true
            } else {
                false
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.GET, docPath)
            false
        }
    }

    /**
     * Generates a fresh 6-digit OTP, emails it to the user's Google account inbox via Resend API,
     * and stores only its SHA-256 hash in `/otp_verifications/{uid}`.
     */
    suspend fun sendOtpToGoogleEmail(user: FirebaseUser): OtpSendResult {
        val uid = user.uid
        val email = user.email?.trim().orEmpty()
        if (uid.isBlank() || email.isBlank()) {
            return OtpSendResult.Error("Google account email not available. Please sign in again.")
        }

        val deviceId = identityStore.getOrCreateDeviceId()
        val otpCode = generateSixDigitOtp()
        val otpHash = computeOtpHash(uid, deviceId, otpCode)
        val expiresAtEpochMs = System.currentTimeMillis() + OTP_VALIDITY_MS

        val apiKey = BuildConfig.RESEND_API_KEY.trim()
        val fromEmail = BuildConfig.OTP_FROM_EMAIL.trim().ifBlank { "onboarding@resend.dev" }
        val isApiKeyConfigured = apiKey.isNotBlank() && apiKey != PLACEHOLDER_RESEND_KEY

        var deliveryStatus = "PENDING"
        var deliveryFailureReason: String? = null

        if (isApiKeyConfigured) {
            val sendEmailResult = deliverOtpEmailViaResend(
                apiKey = apiKey,
                fromEmail = fromEmail,
                toEmail = email,
                otpCode = otpCode,
                displayName = user.displayName.orEmpty(),
                deviceId = deviceId
            )
            sendEmailResult.onSuccess {
                deliveryStatus = "SENT"
            }.onFailure { err ->
                deliveryStatus = "FAILED"
                deliveryFailureReason = err.localizedMessage ?: "Email delivery failed"
            }
        }

        // Persist hashed OTP challenge in Firestore (strictly aligned with firestore.rules create/update)
        val docPath = "$OTP_COLLECTION/$uid"
        try {
            val docRef = db.collection(OTP_COLLECTION).document(uid)
            val existing = docRef.get().await()
            if (!existing.exists()) {
                val createPayload = mapOf(
                    "email" to email,
                    "deviceId" to deviceId,
                    "otpHash" to otpHash,
                    "verified" to false,
                    "attempts" to 0L,
                    "expiresAtEpochMs" to expiresAtEpochMs,
                    "deliveryStatus" to deliveryStatus,
                    "createdAt" to FieldValue.serverTimestamp(),
                    "updatedAt" to FieldValue.serverTimestamp()
                )
                docRef.set(createPayload).await()
            } else {
                val updatePayload = mapOf(
                    "deviceId" to deviceId,
                    "otpHash" to otpHash,
                    "verified" to false,
                    "attempts" to 0L,
                    "expiresAtEpochMs" to expiresAtEpochMs,
                    "deliveryStatus" to deliveryStatus,
                    "updatedAt" to FieldValue.serverTimestamp()
                )
                docRef.update(updatePayload).await()
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.WRITE, docPath)
            return OtpSendResult.Error(e.localizedMessage ?: "Failed to save OTP challenge")
        }

        return when {
            !isApiKeyConfigured -> OtpSendResult.EmailServiceNotConfigured(email)
            deliveryStatus == "SENT" -> OtpSendResult.Sent(email)
            else -> OtpSendResult.Error(deliveryFailureReason ?: "Unable to send OTP email to $email")
        }
    }

    /**
     * Verifies a pasted or typed 6-digit OTP against the SHA-256 hash stored in Firestore.
     */
    suspend fun verifyOtpCode(user: FirebaseUser, rawInput: String): OtpVerifyResult {
        val uid = user.uid
        val email = user.email?.trim().orEmpty()
        val digits = extractSixDigitOtp(rawInput)
        if (digits.length != 6) {
            return OtpVerifyResult.Error("Please paste or enter a valid 6-digit OTP code.")
        }

        val deviceId = identityStore.getOrCreateDeviceId()
        val docPath = "$OTP_COLLECTION/$uid"
        return try {
            val docRef = db.collection(OTP_COLLECTION).document(uid)
            val snap = docRef.get().await()
            if (!snap.exists()) {
                return OtpVerifyResult.Error("No active OTP found. Tap Resend OTP first.")
            }

            val storedHash = snap.getString("otpHash").orEmpty()
            val storedDeviceId = snap.getString("deviceId").orEmpty()
            val attempts = (snap.getLong("attempts") ?: 0L).toInt()
            val expiresAtEpochMs = snap.getLong("expiresAtEpochMs") ?: 0L

            if (System.currentTimeMillis() > expiresAtEpochMs) {
                return OtpVerifyResult.Expired
            }
            if (attempts >= MAX_OTP_ATTEMPTS) {
                return OtpVerifyResult.TooManyAttempts
            }

            val candidateHash = computeOtpHash(uid, storedDeviceId.ifBlank { deviceId }, digits)
            if (candidateHash == storedHash) {
                docRef.update(
                    mapOf(
                        "verified" to true,
                        "attempts" to (attempts + 1).toLong(),
                        "updatedAt" to FieldValue.serverTimestamp()
                    )
                ).await()
                identityStore.markOtpVerified(uid, email)
                OtpVerifyResult.Verified
            } else {
                val nextAttempts = (attempts + 1).coerceAtMost(MAX_OTP_ATTEMPTS + 1)
                docRef.update(
                    mapOf(
                        "attempts" to nextAttempts.toLong(),
                        "updatedAt" to FieldValue.serverTimestamp()
                    )
                ).await()
                val remaining = (MAX_OTP_ATTEMPTS - nextAttempts).coerceAtLeast(0)
                if (remaining == 0) OtpVerifyResult.TooManyAttempts else OtpVerifyResult.InvalidCode(remaining)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.UPDATE, docPath)
            OtpVerifyResult.Error(e.localizedMessage ?: "Verification failed. Please retry.")
        }
    }

    private suspend fun deliverOtpEmailViaResend(
        apiKey: String,
        fromEmail: String,
        toEmail: String,
        otpCode: String,
        displayName: String,
        deviceId: String
    ): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            val greeting = if (displayName.isNotBlank()) "Hi $displayName," else "Hello,"
            val htmlBody = """
                <div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;padding:24px;background:#10141A;color:#F2F6FA;border-radius:16px;border:1px solid #2A323D;">
                  <h2 style="color:#FFBE0B;margin:0 0 8px;">Mamun Walkie Talkie</h2>
                  <p style="color:#9BA8B7;margin:0 0 20px;font-size:14px;">$greeting use this 6-digit verification code to connect your phone ($deviceId) to the private family radio network:</p>
                  <div style="background:#8FE84E;color:#0A1806;font-family:monospace;font-size:34px;font-weight:bold;letter-spacing:8px;text-align:center;padding:18px;border-radius:12px;">$otpCode</div>
                  <p style="color:#9BA8B7;margin:20px 0 0;font-size:12px;">Copy this code and paste it inside the Mamun Walkie Talkie app. This OTP expires in 10 minutes.</p>
                </div>
            """.trimIndent()

            val plainText = "$greeting\n\nYour Mamun Walkie Talkie 6-digit OTP code is: $otpCode\n\nDevice ID: $deviceId\nCopy & paste this code inside the app to verify and connect. Expires in 10 minutes."

            val json = JSONObject().apply {
                put("from", "Mamun Walkie Talkie <$fromEmail>")
                put("to", JSONArray().put(toEmail))
                put("subject", "Mamun Walkie Talkie OTP: $otpCode")
                put("html", htmlBody)
                put("text", plainText)
            }

            val body = json.toString().toRequestBody("application/json; charset=utf-8".toMediaType())
            val request = Request.Builder()
                .url("https://api.resend.com/emails")
                .header("Authorization", "Bearer $apiKey")
                .header("Content-Type", "application/json")
                .post(body)
                .build()

            httpClient.newCall(request).execute().use { response ->
                if (response.isSuccessful) {
                    Log.i(OTP_TAG, "OTP email delivered successfully to $toEmail")
                    Result.success(Unit)
                } else {
                    val errText = response.body?.string().orEmpty()
                    val parsedMsg = runCatching { JSONObject(errText).optString("message") }
                        .getOrNull()
                        ?.takeIf { it.isNotBlank() }
                        ?: "Email API error (HTTP ${response.code})"
                    Log.w(OTP_TAG, "Resend API rejected email (${response.code}): $parsedMsg")
                    Result.failure(IllegalStateException(parsedMsg))
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Log.e(OTP_TAG, "Network failure delivering OTP email", e)
            Result.failure(e)
        }
    }

    private fun generateSixDigitOtp(): String {
        val random = SecureRandom()
        val code = 100000 + random.nextInt(900000)
        return code.toString()
    }

    private fun computeOtpHash(uid: String, deviceId: String, otp: String): String {
        val digest = MessageDigest.getInstance("SHA-256")
            .digest("$uid:$deviceId:$otp".toByteArray(Charsets.UTF_8))
        return digest.joinToString("") { "%02x".format(it) }
    }

    companion object {
        /**
         * Extracts the first 6-digit numeric OTP from clipboard text (even if the user copied
         * the entire email sentence like "Your Mamun Walkie Talkie OTP: 482913").
         */
        fun extractSixDigitOtp(raw: String): String {
            val regexMatch = Regex("\\b(\\d{6})\\b").find(raw)
            if (regexMatch != null) return regexMatch.groupValues[1]
            return raw.filter { it.isDigit() }.take(6)
        }
    }
}
