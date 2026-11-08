package com.maxrave.data.mediaservice

import com.maxrave.domain.data.entities.SongEntity
import com.maxrave.domain.manager.DataStoreManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

/**
 * Best-effort now-playing publisher for a personal Cloudflare Worker.
 *
 * Configure the endpoint and bearer token in DataStore using the keys below. Network failures
 * are intentionally swallowed: status publishing must never interrupt audio playback.
 */
internal class CloudflareMusicStatusReporter(
    private val store: DataStoreManager,
    private val scope: CoroutineScope,
) {
    private var heartbeatJob: Job? = null
    @Volatile private var latestSong: SongEntity? = null

    fun onTrackChanged(song: SongEntity, isPlaying: Boolean) {
        latestSong = song
        publish(song, isPlaying)
    }

    fun onPlaybackChanged(isPlaying: Boolean) {
        val song = latestSong ?: return
        if (isPlaying) {
            publish(song, true)
            if (heartbeatJob?.isActive != true) {
                heartbeatJob = scope.launch {
                    while (true) {
                        delay(3 * 60 * 1000L)
                        latestSong?.let { send(it, true) }
                    }
                }
            }
        } else {
            heartbeatJob?.cancel()
            heartbeatJob = null
            publish(song, false)
        }
    }

    fun close() {
        heartbeatJob?.cancel()
        heartbeatJob = null
        latestSong?.let { publish(it, false) }
    }

    private fun publish(song: SongEntity, isPlaying: Boolean) {
        scope.launch { send(song, isPlaying) }
    }

    private suspend fun send(song: SongEntity, isPlaying: Boolean) {
        val endpoint = store.getString(KEY_WORKER_URL).first()?.trim().orEmpty()
            .ifBlank { DEFAULT_WORKER_URL }
        val token = store.getString(KEY_WORKER_TOKEN).first()?.trim().orEmpty()
        if (endpoint.isBlank() || token.isBlank()) return

        val payload = JSONObject()
            .put("isPlaying", isPlaying)
            .put("track", JSONObject()
                .put("id", song.videoId)
                .put("title", song.title)
                .put("artist", song.artistName?.joinToString(", ").orEmpty())
                .put("album", song.albumName.orEmpty())
                .put("artwork", song.thumbnails.orEmpty())
                .put("duration", song.durationSeconds)
                .put("url", "https://music.youtube.com/watch?v=${song.videoId}"))
            .toString()

        withContext(Dispatchers.IO) {
            var connection: HttpURLConnection? = null
            try {
                val base = endpoint.trimEnd('/')
                val target = if (base.endsWith("/update")) base else "$base/update"
                connection = (URL(target).openConnection() as HttpURLConnection).apply {
                    requestMethod = "POST"
                    connectTimeout = 5_000
                    readTimeout = 5_000
                    doOutput = true
                    setRequestProperty("Content-Type", "application/json")
                    setRequestProperty("Authorization", "Bearer $token")
                }
                connection.outputStream.use { it.write(payload.toByteArray(Charsets.UTF_8)) }
                connection.responseCode
            } catch (_: Exception) {
                // Optional integration. Playback must remain unaffected by a Worker outage.
            } finally {
                connection?.disconnect()
            }
        }
    }

    companion object {
        const val KEY_WORKER_URL = "cloudflare_music_status_worker_url"
        const val KEY_WORKER_TOKEN = "cloudflare_music_status_worker_token"
        private const val DEFAULT_WORKER_URL = "https://vinith-music-status.vinithreddy08-123.workers.dev"
    }
}
