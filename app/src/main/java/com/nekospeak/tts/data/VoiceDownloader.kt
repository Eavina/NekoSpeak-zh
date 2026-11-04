package com.nekospeak.tts.data

import android.content.Context
import android.util.Log
import java.io.File
import java.io.FileOutputStream
import java.net.HttpURLConnection
import java.net.URL
import kotlinx.coroutines.flow.MutableStateFlow

data class VoiceDownloadStatus(
    val state: DownloadState,
    val progress: Float // 0.0 to 1.0
)

/**
 * Downloads Piper voice files (onnx + onnx.json) using HttpURLConnection directly.
 *
 * We intentionally do NOT use Android's DownloadManager: it requires a fully
 * "validated" network and can stay stuck in PENDING on networks behind proxies /
 * VPNs (e.g. Clash) or on emulators with partial connectivity. HttpURLConnection
 * works wherever the host can reach the URL, matching ModelRepository which is
 * proven reliable for the Pocket-TTS / Kokoro downloads.
 */
class VoiceDownloader(private val context: Context) {

    companion object {
        private const val TAG = "VoiceDownloader"
        // In-memory tracker for active/recent downloads: voiceId -> status flow
        private val activeDownloads = mutableMapOf<String, MutableStateFlow<VoiceDownloadStatus>>()
    }

    fun downloadVoice(voiceId: String, onnxUrl: String, jsonUrl: String) {
        val existing = activeDownloads[voiceId]
        if (existing != null && existing.value.state == DownloadState.Downloading) {
            Log.i(TAG, "Download already in progress for $voiceId")
            return
        }

        val dir = context.getExternalFilesDir("piper_downloads")
            ?: File(context.filesDir, "piper_downloads")
        dir.mkdirs()

        val onnxFile = File(dir, "$voiceId.onnx")
        val jsonFile = File(dir, "$voiceId.onnx.json")

        // If both files already complete, nothing to do.
        val onnxDone = onnxFile.exists() && onnxFile.length() > 1024
        val jsonDone = jsonFile.exists() && jsonFile.length() > 0
        if (onnxDone && jsonDone) {
            Log.i(TAG, "Voice $voiceId already downloaded")
            activeDownloads[voiceId] = MutableStateFlow(VoiceDownloadStatus(DownloadState.Downloaded, 1.0f))
            return
        }

        val statusFlow = MutableStateFlow(VoiceDownloadStatus(DownloadState.Downloading, 0f))
        activeDownloads[voiceId] = statusFlow

        Thread {
            try {
                // Download the onnx model first (0 -> 0.9 of overall progress). Skip if already complete.
                val onnxOk = if (onnxDone) {
                    statusFlow.value = VoiceDownloadStatus(DownloadState.Downloading, 0.9f)
                    true
                } else {
                    downloadFile(onnxUrl, onnxFile) { p ->
                        statusFlow.value = VoiceDownloadStatus(DownloadState.Downloading, p * 0.9f)
                    }
                }
                if (!onnxOk) {
                    Log.e(TAG, "Failed to download onnx for $voiceId")
                    statusFlow.value = VoiceDownloadStatus(DownloadState.NotAvailable, 0f)
                    return@Thread
                }

                // Download the config json (0.9 -> 1.0). Skip if already complete.
                val jsonOk = if (jsonDone) {
                    statusFlow.value = VoiceDownloadStatus(DownloadState.Downloaded, 1.0f)
                    true
                } else {
                    downloadFile(jsonUrl, jsonFile) { p ->
                        statusFlow.value = VoiceDownloadStatus(DownloadState.Downloading, 0.9f + p * 0.1f)
                    }
                }
                if (!jsonOk) {
                    Log.e(TAG, "Failed to download json for $voiceId")
                    statusFlow.value = VoiceDownloadStatus(DownloadState.NotAvailable, 0f)
                    return@Thread
                }

                Log.i(TAG, "Voice $voiceId downloaded")
                statusFlow.value = VoiceDownloadStatus(DownloadState.Downloaded, 1.0f)
            } catch (e: Exception) {
                Log.e(TAG, "Download failed for $voiceId", e)
                statusFlow.value = VoiceDownloadStatus(DownloadState.NotAvailable, 0f)
            }
        }.apply { name = "voice-download-$voiceId"; start() }
    }

    fun getDownloadStatus(voiceId: String): VoiceDownloadStatus {
        activeDownloads[voiceId]?.let { return it.value }

        // Not actively downloading — fall back to disk existence check.
        val dir = context.getExternalFilesDir("piper_downloads")
            ?: File(context.filesDir, "piper_downloads")
        val onnx = File(dir, "$voiceId.onnx")
        val json = File(dir, "$voiceId.onnx.json")

        return if (onnx.exists() && onnx.length() > 1024 && json.exists() && json.length() > 0) {
            VoiceDownloadStatus(DownloadState.Downloaded, 1.0f)
        } else {
            VoiceDownloadStatus(DownloadState.NotDownloaded, 0f)
        }
    }

    /**
     * Download a single file, following redirects manually (HuggingFace LFS / hf-mirror CDN).
     * Writes to a .tmp file and atomically renames on success so partial downloads are
     * never mistaken for a complete voice.
     */
    private fun downloadFile(urlString: String, targetFile: File, onProgress: (Float) -> Unit): Boolean {
        val tmpFile = File(targetFile.parentFile, targetFile.name + ".tmp")
        var connection: HttpURLConnection? = null
        try {
            var currentUrl = urlString
            var redirectCount = 0
            val maxRedirects = 5

            while (redirectCount < maxRedirects) {
                val url = URL(currentUrl)
                connection = url.openConnection() as HttpURLConnection
                connection.connectTimeout = 30000
                connection.readTimeout = 120000 // 2 min timeout for large files
                connection.requestMethod = "GET"
                connection.setRequestProperty("User-Agent", "NekoSpeak-TTS/1.0")
                connection.instanceFollowRedirects = false // Handle redirects manually

                val responseCode = connection.responseCode
                when (responseCode) {
                    HttpURLConnection.HTTP_OK -> break
                    HttpURLConnection.HTTP_MOVED_TEMP, HttpURLConnection.HTTP_MOVED_PERM,
                    HttpURLConnection.HTTP_SEE_OTHER, 307, 308 -> {
                        val newUrl = connection.getHeaderField("Location")
                        if (newUrl == null) {
                            Log.e(TAG, "Redirect without Location header for $urlString")
                            return false
                        }
                        // Resolve relative redirects (hf-mirror returns relative /api/resolve-cache/... paths)
                        currentUrl = URL(URL(currentUrl), newUrl).toString()
                        connection.disconnect()
                        redirectCount++
                    }
                    else -> {
                        Log.e(TAG, "HTTP error $responseCode for $urlString")
                        return false
                    }
                }
            }

            if (redirectCount >= maxRedirects) {
                Log.e(TAG, "Too many redirects for $urlString")
                return false
            }

            val contentLength = connection!!.contentLengthLong
            var bytesDownloaded = 0L

            connection.inputStream.use { input ->
                FileOutputStream(tmpFile).use { output ->
                    val buffer = ByteArray(8192)
                    var bytesRead: Int
                    while (input.read(buffer).also { bytesRead = it } != -1) {
                        output.write(buffer, 0, bytesRead)
                        bytesDownloaded += bytesRead
                        if (contentLength > 0) {
                            onProgress(bytesDownloaded.toFloat() / contentLength.toFloat())
                        }
                    }
                }
            }

            // Atomic rename so a half-written file never looks "downloaded".
            if (tmpFile.exists()) {
                if (targetFile.exists()) targetFile.delete()
                tmpFile.renameTo(targetFile)
            }
            onProgress(1f)
            return true
        } catch (e: Exception) {
            Log.e(TAG, "Download error for $urlString", e)
            tmpFile.delete()
            return false
        } finally {
            connection?.disconnect()
        }
    }
}
