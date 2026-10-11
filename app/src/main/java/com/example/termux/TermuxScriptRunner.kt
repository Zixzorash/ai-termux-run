package com.example.termux

import android.app.PendingIntent
import android.content.ClipData
import android.content.ClipboardManager
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.os.Environment
import android.os.SystemClock
import com.example.data.local.AppDatabase
import com.example.data.model.ExecutionLog
import com.example.data.model.TermuxScriptFile
import com.example.receiver.TermuxResultReceiver
import com.example.service.TermuxNotificationHelper
import com.example.utils.SafUtils
import com.example.utils.StoragePermissionHelper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.io.OutputStreamWriter

enum class ExecutionMode {
    AUTO,
    TERMUX_SERVICE,
    IN_APP_DIRECT
}

data class ScriptRunState(
    val isRunning: Boolean = false,
    val scriptName: String = "",
    val logicalPath: String = "",
    val outputLogs: List<String> = emptyList(),
    val exitCode: Int? = null,
    val durationMs: Long = 0,
    val statusMessage: String = "",
    val supportsLiveOutput: Boolean = true,
    val activeScriptFile: TermuxScriptFile? = null,
    val isWaitingForInput: Boolean = false
)

class TermuxScriptRunner private constructor(private val context: Context) {

    private val commandManager = TermuxCommandManager(context)
    private val notificationHelper = TermuxNotificationHelper(context)
    private val scope = CoroutineScope(Dispatchers.IO)
    private val dao = AppDatabase.getDatabase(context).termuxDao()

    private val _runState = MutableStateFlow(ScriptRunState())
    val runState: StateFlow<ScriptRunState> = _runState.asStateFlow()

    @Volatile
    private var activeProcess: Process? = null
    @Volatile
    private var activeProcessWriter: OutputStreamWriter? = null

    companion object {
        @Volatile
        private var INSTANCE: TermuxScriptRunner? = null

        fun getInstance(context: Context): TermuxScriptRunner {
            return INSTANCE ?: synchronized(this) {
                val instance = TermuxScriptRunner(context.applicationContext)
                INSTANCE = instance
                instance
            }
        }
    }

    /**
     * Executes a Termux script file.
     * Can execute via Direct In-App Runner (with interactive live response) or Termux RUN_COMMAND service.
     */
    fun runScript(script: TermuxScriptFile, mode: ExecutionMode = ExecutionMode.AUTO) {
        scope.launch {
            val startTime = SystemClock.elapsedRealtime()
            val startTimestamp = System.currentTimeMillis()

            // 1. Security validation: Anti-traversal & Anti-injection check
            if (!script.isNameSafe) {
                withContext(Dispatchers.Main) {
                    _runState.value = ScriptRunState(
                        isRunning = false,
                        scriptName = script.name,
                        logicalPath = script.logicalPosixPath,
                        outputLogs = listOf(
                            "❌ ปฏิเสธการรันคำสั่ง: ชื่อไฟล์สคริปต์ไม่ปลอดภัย (อาจมี ../ หรือ shell injection characters)",
                            "ชื่อไฟล์: ${script.name}"
                        ),
                        exitCode = -1,
                        statusMessage = "ความปลอดภัย: ปฏิเสธการรันชื่อไฟล์ที่อาจก่อให้เกิดอันตราย",
                        activeScriptFile = script,
                        isWaitingForInput = false
                    )
                }
                return@launch
            }

            // 2. Prepare initial state & notification
            val initialLogs = mutableListOf(
                "🚀 [${java.text.SimpleDateFormat("HH:mm:ss", java.util.Locale.getDefault()).format(java.util.Date())}] เริ่มต้นการรันสคริปต์...",
                "📜 สคริปต์: ${script.name}",
                "📍 Logical Path: ${script.logicalPosixPath}",
                "⚡ กำลังเริ่มโปรเซส (พิมพ์ตอบกลับสคริปต์สดได้ทันที)..."
            )

            withContext(Dispatchers.Main) {
                _runState.value = ScriptRunState(
                    isRunning = true,
                    scriptName = script.name,
                    logicalPath = script.logicalPosixPath,
                    outputLogs = initialLogs,
                    statusMessage = "กำลังประมวลผลคำสั่ง...",
                    activeScriptFile = script,
                    isWaitingForInput = false
                )
            }

            notificationHelper.showRunningNotification(
                scriptId = script.name.hashCode().toLong(),
                scriptName = script.name,
                commandPreview = script.logicalPosixPath
            )

            // 3. Execution routing
            when (mode) {
                ExecutionMode.IN_APP_DIRECT -> {
                    executeDirectlyInApp(script, startTime, startTimestamp)
                }
                ExecutionMode.TERMUX_SERVICE -> {
                    executeViaRunCommandService(script, startTime, startTimestamp, fallbackToDirect = false, interactive = true)
                }
                ExecutionMode.AUTO -> {
                    // Check if the script content calls FFmpeg, FFprobe, or other Termux-native packages
                    val scriptContent = try {
                        SafUtils.readDocumentContent(context.contentResolver, script.uri)
                    } catch (_: Exception) { "" }

                    val requiresTermuxPackages = scriptContent.contains("ffmpeg", ignoreCase = true) ||
                            scriptContent.contains("ffprobe", ignoreCase = true) ||
                            scriptContent.contains("pkg install", ignoreCase = true) ||
                            scriptContent.contains("pkg update", ignoreCase = true) ||
                            scriptContent.contains("apt-get", ignoreCase = true) ||
                            scriptContent.contains("yt-dlp", ignoreCase = true) ||
                            scriptContent.contains("mediainfo", ignoreCase = true)

                    val termuxInfo = commandManager.checkTermuxInstallation()
                    val hasTermux = termuxInfo !is TermuxVersionInfo.NotInstalled

                    val prefs = context.getSharedPreferences("termux_runner_prefs", Context.MODE_PRIVATE)
                    val configuredEngine = prefs.getString("default_exec_engine", "IN_APP_DIRECT") ?: "IN_APP_DIRECT"

                    if (requiresTermuxPackages && hasTermux) {
                        appendLog("🎬 ตรวจพบการเรียกใช้ FFmpeg / FFprobe หรือแพ็กเกจของ Termux ในสคริปต์ ${script.name}")
                        appendLog("🚀 สลับไปสั่งรันผ่าน Termux โดยอัตโนมัติ เพื่อใช้งานฮาร์ดแวร์ GPU+CPU และแพ็กเกจ FFmpeg จริง...")
                        executeViaRunCommandService(script, startTime, startTimestamp, fallbackToDirect = false, interactive = true)
                    } else if (configuredEngine == "TERMUX_SERVICE") {
                        executeViaRunCommandService(script, startTime, startTimestamp, fallbackToDirect = true, interactive = true)
                    } else {
                        executeDirectlyInApp(script, startTime, startTimestamp)
                    }
                }
            }
        }
    }

    /**
     * Executes the script via Termux RUN_COMMAND service.
     * When interactive = true, opens Termux foreground terminal session with session action "0"
     * so Termux automatically creates a new terminal session running the script in foreground.
     */
    suspend fun executeViaRunCommandService(
        script: TermuxScriptFile,
        startTime: Long,
        startTimestamp: Long,
        fallbackToDirect: Boolean,
        interactive: Boolean = true
    ) {
        val termuxInfo = commandManager.checkTermuxInstallation()
        if (termuxInfo is TermuxVersionInfo.NotInstalled) {
            appendLog("⚠️ ไม่พบแอพ Termux ในเครื่อง สลับไปรันโดยตรงในแอพทันที...")
            executeDirectlyInApp(script, startTime, startTimestamp)
            return
        }

        appendLog("⚡ เตรียมส่งคำสั่ง '${script.name}' ไปยัง Termux...")

        // 1. Check com.termux.permission.RUN_COMMAND permission status
        val hasRunPermission = StoragePermissionHelper.hasTermuxRunCommandPermission(context)
        if (!hasRunPermission) {
            appendLog("⚠️ คำเตือน: แอพยังไม่ได้รับสิทธิ์ 'com.termux.permission.RUN_COMMAND'")
            appendLog("💡 หาก Termux ไม่รันอัตโนมัติ ให้ไปที่ ข้อมูลแอพนี้ -> สิทธิ์ (Permissions) -> สิทธิ์เพิ่มเติม -> อนุญาต 'สั่งการคำสั่งใน Termux'")
        }

        // 2. Read script content and ensure a clean Unix-formatted copy is cached in /sdcard/Download/.termux_runner/
        // so Termux bash can reliably access and run it even if Termux home/SAF paths vary
        val scriptContent = try {
            SafUtils.readDocumentContent(context.contentResolver, script.uri)
        } catch (_: Exception) { "" }

        val cachedSharedScriptPath = try {
            if (scriptContent.isNotBlank()) {
                val downloadsDir = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
                val runnerCacheDir = File(downloadsDir, ".termux_runner")
                if (!runnerCacheDir.exists()) {
                    runnerCacheDir.mkdirs()
                }
                val cachedFile = File(runnerCacheDir, script.name)
                val unixScript = scriptContent.replace("\r\n", "\n").replace("\r", "\n")
                cachedFile.writeText(unixScript, Charsets.UTF_8)
                cachedFile.setExecutable(true, false)
                cachedFile.setReadable(true, false)
                cachedFile.absolutePath
            } else {
                "/sdcard/Download/.termux_runner/${script.name}"
            }
        } catch (e: Exception) {
            "/sdcard/Download/.termux_runner/${script.name}"
        }

        // 3. Always copy the primary execution command to clipboard as an instant backup for the user
        val fallbackCommand = "bash ~/.shortcuts/${script.name}"
        try {
            val clipboard = context.getSystemService(Context.CLIPBOARD_SERVICE) as? ClipboardManager
            clipboard?.setPrimaryClip(ClipData.newPlainText("Termux Command", fallbackCommand))
            appendLog("📋 คัดลอกคำสั่งสำรองไว้ในคลิปบอร์ดแล้ว: $fallbackCommand")
        } catch (_: Exception) {}

        val scriptId = script.name.hashCode().toLong()

        val callbackIntent = Intent(context, TermuxResultReceiver::class.java).apply {
            action = TermuxResultReceiver.ACTION_COMMAND_RESULT
            putExtra("EXTRA_SCRIPT_ID", scriptId)
            putExtra("EXTRA_SCRIPT_NAME", script.name)
            putExtra("EXTRA_START_TIME", startTime)
            putExtra("EXTRA_TIMESTAMP", startTimestamp)
        }

        val pendingIntent = PendingIntent.getBroadcast(
            context,
            (scriptId and 0xFFFF).toInt(),
            callbackIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE
        )

        val prefs = context.getSharedPreferences("termux_runner_prefs", Context.MODE_PRIVATE)
        val prefixPath = prefs.getString("prefix_path", "/data/data/com.termux/files/usr")?.trim()?.ifEmpty { "/data/data/com.termux/files/usr" } ?: "/data/data/com.termux/files/usr"
        val bashPath = "$prefixPath/bin/bash"
        val scriptFullPath = "/data/data/com.termux/files/home/${script.relativePath}"
        val homeDir = "/data/data/com.termux/files/home"

        // Build a robust shell wrapper that tries Termux home/shortcuts first, then the cached shared copy
        val executionWrapperCmd = buildString {
            append("cd \"$homeDir\" 2>/dev/null || cd /sdcard ; ")
            append("echo \"========================================\" ; ")
            append("echo \"🚀 เริ่มรันสคริปต์: ${script.name}\" ; ")
            append("echo \"========================================\" ; ")
            append("if [ -f \"$scriptFullPath\" ]; then ")
            append("  bash \"$scriptFullPath\" ; ")
            append("elif [ -f \"$homeDir/.shortcuts/${script.name}\" ]; then ")
            append("  bash \"$homeDir/.shortcuts/${script.name}\" ; ")
            append("elif [ -f \"$homeDir/${script.relativePath}\" ]; then ")
            append("  bash \"$homeDir/${script.relativePath}\" ; ")
            append("elif [ -f \"$homeDir/${script.name}\" ]; then ")
            append("  bash \"$homeDir/${script.name}\" ; ")
            append("elif [ -f \"$cachedSharedScriptPath\" ]; then ")
            append("  bash \"$cachedSharedScriptPath\" ; ")
            append("elif [ -f \"/sdcard/Download/.termux_runner/${script.name}\" ]; then ")
            append("  bash \"/sdcard/Download/.termux_runner/${script.name}\" ; ")
            append("elif [ -f \"/storage/emulated/0/Download/.termux_runner/${script.name}\" ]; then ")
            append("  bash \"/storage/emulated/0/Download/.termux_runner/${script.name}\" ; ")
            append("else ")
            append("  echo \"❌ ไม่พบไฟล์ ${script.name} ในพื้นที่ Termux หรือแคช\" ; ")
            append("fi")
        }

        val runArgs = arrayOf("-c", executionWrapperCmd)

        // CRITICAL FOR AUTOMATIC EXECUTION:
        // EXTRA_RUN_COMMAND_SESSION_ACTION MUST be "0" (VALUE_EXTRA_SESSION_ACTION_SWITCH_TO_NEW_SESSION_AND_OPEN_ACTIVITY)
        // so Termux creates a new terminal session, switches to it, and opens the foreground terminal window.
        // EXTRA_RUN_COMMAND_BACKGROUND MUST be false for interactive execution.
        val termuxIntent = Intent(TermuxCommandManager.ACTION_RUN_COMMAND).apply {
            component = ComponentName(TermuxCommandManager.TERMUX_PACKAGE, TermuxCommandManager.RUN_COMMAND_SERVICE)
            putExtra(TermuxCommandManager.EXTRA_RUN_COMMAND_PATH, bashPath)
            putExtra(TermuxCommandManager.EXTRA_RUN_COMMAND_ARGUMENTS, runArgs)
            putExtra(TermuxCommandManager.EXTRA_RUN_COMMAND_WORKDIR, homeDir)
            putExtra(TermuxCommandManager.EXTRA_RUN_COMMAND_BACKGROUND, !interactive)
            putExtra(TermuxCommandManager.EXTRA_RUN_COMMAND_SESSION_ACTION, "0")
            putExtra(TermuxCommandManager.EXTRA_RUN_COMMAND_PENDING_INTENT, pendingIntent)
        }

        try {
            appendLog("⚡ ส่งคำสั่ง RUN_COMMAND ไปยัง Termux RunCommandService...")
            context.startService(termuxIntent)
            appendLog("✅ ส่งคำสั่ง RUN_COMMAND ไปยัง Termux สำเร็จแล้ว!")

            if (interactive) {
                appendLog("🖥️ Termux กำลังเปิดหน้าต่าง Terminal พร้อมรันคำสั่ง '${script.name}' อัตโนมัติ")
                appendLog("⚡ ใช้งานเอนจิน FFmpeg/FFprobe พร้อมฮาร์ดแวร์เร่งความเร็ว GPU+CPU ใน Termux เต็มกำลัง")
            } else {
                appendLog("⏳ กำลังรอผลลัพธ์จาก Termux Callback...")
            }

            val duration = SystemClock.elapsedRealtime() - startTime
            withContext(Dispatchers.Main) {
                _runState.value = _runState.value.copy(
                    isRunning = false,
                    durationMs = duration,
                    statusMessage = "ส่งคำสั่ง '${script.name}' ไปยัง Termux สำเร็จ (รันใน Termux จริง)",
                    supportsLiveOutput = true,
                    isWaitingForInput = false
                )
            }
        } catch (e: SecurityException) {
            appendLog("🔴 ระบบ Android ปฏิเสธการส่ง Intent Service: ขาดสิทธิ์ com.termux.permission.RUN_COMMAND")
            appendLog("👉 กรุณาเปิดสิทธิ์: ข้อมูลแอพนี้ -> สิทธิ์ (Permissions) -> สิทธิ์เพิ่มเติม -> สั่งการคำสั่งใน Termux")
            appendLog("📋 ได้คัดลอกคำสั่งสำรองไว้ในคลิปบอร์ดแล้ว สามารถกดเปิด Termux แล้ว Paste เพื่อรันได้ทันที")
            try {
                val launchIntent = commandManager.getLaunchTermuxIntent()?.apply {
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                if (launchIntent != null) {
                    context.startActivity(launchIntent)
                }
            } catch (_: Exception) {}

            if (fallbackToDirect) {
                appendLog("🔄 กำลังสลับไปรันโดยตรงในแอพให้ทันที...")
                executeDirectlyInApp(script, startTime, startTimestamp)
            } else {
                finishWithError(script, -1, "ขาดสิทธิ์ com.termux.permission.RUN_COMMAND: ${e.message}")
            }
        } catch (e: Exception) {
            appendLog("⚠️ ไม่สามารถรันผ่าน Termux Service: ${e.message}")
            // If starting the service directly failed, try to launch Termux and copy command
            try {
                val launchIntent = commandManager.getLaunchTermuxIntent()?.apply {
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                if (launchIntent != null) {
                    context.startActivity(launchIntent)
                }
            } catch (_: Exception) {}

            if (fallbackToDirect) {
                appendLog("🔄 กำลังสลับไปรันสคริปต์โดยตรงในแอพนี้ให้ทันที...")
                executeDirectlyInApp(script, startTime, startTimestamp)
            } else {
                finishWithError(script, -1, "ไม่สามารถเรียก Termux Service: ${e.message}")
            }
        }
    }

    /**
     * Executes the script directly inside this app process runner,
     * reading contents from SAF and streaming real stdout & stderr line by line.
     * Fully supports interactive STDIN responses (e.g. read -p "Enter filename: ").
     */
    suspend fun executeDirectlyInApp(
        script: TermuxScriptFile,
        startTime: Long = SystemClock.elapsedRealtime(),
        startTimestamp: Long = System.currentTimeMillis()
    ) {
        appendLog("⚙️ เริ่มต้นรันสคริปต์โดยตรงในแอพ (Direct In-App Engine)...")

        val scriptContent = try {
            SafUtils.readDocumentContent(context.contentResolver, script.uri)
        } catch (e: Exception) {
            appendLog("❌ ไม่สามารถอ่านเนื้อหาไฟล์จาก SAF: ${e.message}")
            finishWithError(script, -1, "Cannot read SAF document")
            return
        }

        if (scriptContent.isBlank()) {
            appendLog("⚠️ ไฟล์สคริปต์ว่างเปล่า ไม่มีคำสั่งให้ทำงาน")
            finishWithError(script, 0, "Empty script")
            return
        }

        // Prepare script content with Bash compatibility layer (supporting read -p on mksh/Android sh)
        val prefs = context.getSharedPreferences("termux_runner_prefs", Context.MODE_PRIVATE)
        val prefixPath = prefs.getString("prefix_path", "/data/data/com.termux/files/usr")?.trim()?.ifEmpty { "/data/data/com.termux/files/usr" } ?: "/data/data/com.termux/files/usr"
        val prefixBinDir = "$prefixPath/bin"
        val prefixLibDir = "$prefixPath/lib"
        val compatibleScriptContent = injectBashCompatibility(scriptContent, prefixPath)

        // Write to temporary executable cache file in app private cache
        val scriptDir = File(context.cacheDir, "script_exec_cache").apply { mkdirs() }
        val tempScriptFile = File(scriptDir, "run_${System.currentTimeMillis()}.sh")
        try {
            tempScriptFile.writeText(compatibleScriptContent, Charsets.UTF_8)
            tempScriptFile.setExecutable(true, false)

            appendLog("▶️ กำลังประมวลผล Shell Script (พร้อมรับคำตอบผ่านแอพโดยตรง)...")

            val termuxHomeDir = File("/data/data/com.termux/files/home")
            val sdcardDir = File("/sdcard")
            val storageDir = File("/storage/emulated/0")

            val baseWorkingDir = when {
                termuxHomeDir.exists() && termuxHomeDir.canRead() -> termuxHomeDir
                sdcardDir.exists() && sdcardDir.canWrite() -> sdcardDir
                storageDir.exists() && storageDir.canWrite() -> storageDir
                else -> context.filesDir
            }

            val pb = ProcessBuilder("sh", tempScriptFile.absolutePath)
                .directory(baseWorkingDir)

            val termuxHomePath = if (termuxHomeDir.exists()) "/data/data/com.termux/files/home" else baseWorkingDir.absolutePath

            pb.environment().apply {
                put("HOME", termuxHomePath)
                put("TERMUX_HOME", "/data/data/com.termux/files/home")
                put("PREFIX", prefixPath)
                put("TERMUX_PREFIX", prefixPath)
                put("TERMUX_APP_FILES", context.filesDir.absolutePath)
                put("TMPDIR", context.cacheDir.absolutePath)
                put("TERM", "xterm-256color")
                put("LANG", "en_US.UTF-8")
                put("LC_ALL", "en_US.UTF-8")
                put("PATH", "$prefixBinDir:/data/data/com.termux/files/usr/bin:/system/bin:/system/xbin:/vendor/bin:/apex/com.android.runtime/bin")
                put("LD_LIBRARY_PATH", "$prefixLibDir:/vendor/lib64:/system/lib64")
                // Device Shared Storage Paths (e.g. /sdcard/Movies/, /storage/emulated/0/Movies/)
                put("EXTERNAL_STORAGE", "/sdcard")
                put("SDCARD", "/sdcard")
                put("STORAGE", "/storage/emulated/0")
                put("MOVIES", "/sdcard/Movies")
                put("DOWNLOADS", "/sdcard/Download")
                put("DOCUMENTS", "/sdcard/Documents")
                put("DCIM", "/sdcard/DCIM")
                put("PICTURES", "/sdcard/Pictures")
                put("MUSIC", "/sdcard/Music")
            }

            val process = pb.start()
            activeProcess = process
            val writer = OutputStreamWriter(process.outputStream, Charsets.UTF_8)
            activeProcessWriter = writer

            val stdoutBuilder = StringBuilder()
            val stderrBuilder = StringBuilder()

            // Read STDOUT in real-time supporting unbuffered prompts (e.g. read -p "Enter filename: ")
            val stdoutJob = scope.launch(Dispatchers.IO) {
                val reader = InputStreamReader(process.inputStream, Charsets.UTF_8)
                val charBuffer = CharArray(512)
                val lineBuffer = StringBuilder()
                var readCount: Int
                while (reader.read(charBuffer).also { readCount = it } != -1) {
                    for (i in 0 until readCount) {
                        val c = charBuffer[i]
                        if (c == '\n' || c == '\r') {
                            if (lineBuffer.isNotEmpty()) {
                                val line = lineBuffer.toString()
                                stdoutBuilder.append(line).append("\n")
                                appendLog("🟢 [OUT] $line")
                                lineBuffer.clear()
                            }
                        } else {
                            lineBuffer.append(c)
                        }
                    }
                    if (lineBuffer.isNotEmpty() && !reader.ready()) {
                        val partial = lineBuffer.toString()
                        stdoutBuilder.append(partial)
                        appendLog("🟢 [PROMPT] $partial")
                        withContext(Dispatchers.Main) {
                            _runState.value = _runState.value.copy(isWaitingForInput = true)
                        }
                        lineBuffer.clear()
                    }
                }
                if (lineBuffer.isNotEmpty()) {
                    val line = lineBuffer.toString()
                    stdoutBuilder.append(line).append("\n")
                    appendLog("🟢 [OUT] $line")
                }
                reader.close()
            }

            // Read STDERR in real-time
            val stderrJob = scope.launch(Dispatchers.IO) {
                val reader = BufferedReader(InputStreamReader(process.errorStream, Charsets.UTF_8))
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    line?.let {
                        stderrBuilder.append(it).append("\n")
                        appendLog("🔴 [ERR] $it")
                    }
                }
                reader.close()
            }

            stdoutJob.join()
            stderrJob.join()

            val exitCode = process.waitFor()
            val duration = SystemClock.elapsedRealtime() - startTime

            appendLog("🏁 สคริปต์ทำงานเสร็จสิ้นด้วยรหัสสถานะ: $exitCode (ใช้เวลา ${duration}ms)")

            withContext(Dispatchers.Main) {
                _runState.value = _runState.value.copy(
                    isRunning = false,
                    exitCode = exitCode,
                    durationMs = duration,
                    statusMessage = if (exitCode == 0) "ทำงานสำเร็จ (0)" else "ล้มเหลว ($exitCode)",
                    supportsLiveOutput = true,
                    isWaitingForInput = false
                )
            }

            // Save to execution logs
            dao.insertLog(
                ExecutionLog(
                    scriptId = script.name.hashCode().toLong(),
                    scriptName = script.name,
                    commandExecuted = "sh ${script.name}",
                    stdout = stdoutBuilder.toString(),
                    stderr = stderrBuilder.toString(),
                    exitCode = exitCode,
                    durationMs = duration,
                    timestamp = startTimestamp,
                    executionMode = "Direct In-App Shell"
                )
            )

            notificationHelper.showCompletedNotification(
                scriptId = script.name.hashCode().toLong(),
                scriptName = script.name,
                exitCode = exitCode,
                durationMs = duration
            )

        } catch (e: Exception) {
            appendLog("❌ เกิดข้อผิดพลาดขณะรันคำสั่ง: ${e.message}")
            finishWithError(script, -1, e.message ?: "Execution error")
        } finally {
            activeProcess = null
            activeProcessWriter = null
            tempScriptFile.delete()
        }
    }

    /**
     * Sends interactive STDIN input (e.g. filename, answer, text) to the running script process.
     */
    fun sendInput(input: String) {
        scope.launch(Dispatchers.IO) {
            val proc = activeProcess
            val writer = activeProcessWriter
            if (proc != null && proc.isAlive && writer != null) {
                try {
                    writer.write(input + "\n")
                    writer.flush()
                    appendLog("💬 [IN] $input")
                    withContext(Dispatchers.Main) {
                        _runState.value = _runState.value.copy(isWaitingForInput = false)
                    }
                } catch (e: Exception) {
                    appendLog("⚠️ ข้อผิดพลาดในการส่ง Input: ${e.message}")
                }
            } else {
                appendLog("⚠️ ไม่มีโปรเซสที่กำลังรอรับ Input ในขณะนี้")
            }
        }
    }

    /**
     * Terminates the active in-app script process if running.
     */
    fun stopExecution() {
        scope.launch {
            try {
                activeProcess?.destroyForcibly()
                activeProcess = null
                activeProcessWriter = null
                appendLog("⏹️ ยกเลิกการทำงานของสคริปต์แล้ว")
                withContext(Dispatchers.Main) {
                    _runState.value = _runState.value.copy(
                        isRunning = false,
                        statusMessage = "ยกเลิกการทำงานแล้ว",
                        isWaitingForInput = false
                    )
                }
            } catch (e: Exception) {
                appendLog("⚠️ ไม่สามารถยกเลิกโปรเซส: ${e.message}")
            }
        }
    }

    private suspend fun appendLog(logLine: String) {
        withContext(Dispatchers.Main) {
            val current = _runState.value.outputLogs.toMutableList()
            current.add(logLine)
            _runState.value = _runState.value.copy(outputLogs = current)
        }
    }

    private suspend fun finishWithError(script: TermuxScriptFile, exitCode: Int, reason: String) {
        withContext(Dispatchers.Main) {
            _runState.value = _runState.value.copy(
                isRunning = false,
                exitCode = exitCode,
                statusMessage = reason,
                isWaitingForInput = false
            )
        }
        notificationHelper.cancelNotification(script.name.hashCode().toLong())
    }

    fun handleExecutionResult(
        scriptId: Long,
        scriptName: String,
        stdout: String,
        stderr: String,
        exitCode: Int,
        startTime: Long,
        timestamp: Long
    ) {
        scope.launch {
            val duration = if (startTime > 0) SystemClock.elapsedRealtime() - startTime else 250L

            appendLog("📥 ได้รับผลลัพธ์จาก Termux Callback:")
            if (stdout.isNotBlank()) appendLog("🟢 [STDOUT]\n$stdout")
            if (stderr.isNotBlank()) appendLog("🔴 [STDERR]\n$stderr")
            appendLog("🏁 รหัสสถานะ (Exit Code): $exitCode (ใช้เวลา ${duration}ms)")

            withContext(Dispatchers.Main) {
                _runState.value = _runState.value.copy(
                    isRunning = false,
                    exitCode = exitCode,
                    durationMs = duration,
                    statusMessage = if (exitCode == 0) "ทำงานสำเร็จ (0)" else "ล้มเหลว ($exitCode)",
                    isWaitingForInput = false
                )
            }

            dao.insertLog(
                ExecutionLog(
                    scriptId = scriptId,
                    scriptName = scriptName,
                    commandExecuted = "com.termux.RUN_COMMAND",
                    stdout = stdout,
                    stderr = stderr,
                    exitCode = exitCode,
                    durationMs = duration,
                    timestamp = timestamp,
                    executionMode = "Termux RUN_COMMAND Callback"
                )
            )

            notificationHelper.showCompletedNotification(
                scriptId = scriptId,
                scriptName = scriptName,
                exitCode = exitCode,
                durationMs = duration
            )
        }
    }

    fun resetRunState() {
        _runState.value = ScriptRunState()
    }

    /**
     * Injects a Bash-compatibility layer into user shell scripts for Android mksh/sh environments.
     * This adds support for `read -p "Prompt: " var` and common bash extensions without crashing.
     */
    private fun injectBashCompatibility(originalContent: String, prefixPath: String = "/data/data/com.termux/files/usr"): String {
        val compatLayer = buildString {
            appendLine("# === AI Studio Bash Compatibility Layer for Android Shell (mksh/sh) ===")
            appendLine("read() {")
            appendLine("    _prompt=\"\"")
            appendLine("    _raw=\"\"")
            appendLine("    _vars=\"\"")
            appendLine("    while [ $# -gt 0 ]; do")
            appendLine("        case \"$1\" in")
            appendLine("            -p)")
            appendLine("                _prompt=\"$2\"")
            appendLine("                shift 2 2>/dev/null || shift")
            appendLine("                ;;")
            appendLine("            -r)")
            appendLine("                _raw=\"-r\"")
            appendLine("                shift")
            appendLine("                ;;")
            appendLine("            -s|-e)")
            appendLine("                shift")
            appendLine("                ;;")
            appendLine("            -n|-t|-u|-d)")
            appendLine("                shift 2 2>/dev/null || shift")
            appendLine("                ;;")
            appendLine("            -*)")
            appendLine("                shift")
            appendLine("                ;;")
            appendLine("            *)")
            appendLine("                _vars=\"\$_vars \$1\"")
            appendLine("                shift")
            appendLine("                ;;")
            appendLine("        esac")
            appendLine("    done")
            appendLine("    if [ -n \"\$_prompt\" ]; then")
            appendLine("        printf \"%b\" \"\$_prompt\"")
            appendLine("    fi")
            appendLine("    if [ -z \"\$_vars\" ]; then")
            appendLine("        command read \$_raw REPLY")
            appendLine("    else")
            appendLine("        command read \$_raw \$_vars")
            appendLine("    fi")
            appendLine("}")
            appendLine("cd() {")
            appendLine("    _target=\"\$1\"")
            appendLine("    if [ -z \"\$_target\" ] || [ \"\$_target\" = \"~\" ] || [ \"\$_target\" = \"\$HOME\" ]; then")
            appendLine("        if [ -d \"/data/data/com.termux/files/home\" ]; then")
            appendLine("            _target=\"/data/data/com.termux/files/home\"")
            appendLine("        elif [ -d \"/sdcard\" ]; then")
            appendLine("            _target=\"/sdcard\"")
            appendLine("        else")
            appendLine("            _target=\"/storage/emulated/0\"")
            appendLine("        fi")
            appendLine("    fi")
            appendLine("    case \"\$_target\" in")
            appendLine("        /data/user/0/*|/data/data/com.aistudio.*)")
            appendLine("            _subpath=\$(echo \"\$_target\" | sed -E 's|/data/(user/0|data)/com.aistudio.[^/]+/files/?||g')")
            appendLine("            if [ -n \"\$_subpath\" ] && [ -d \"/data/data/com.termux/files/home/\$_subpath\" ]; then")
            appendLine("                _target=\"/data/data/com.termux/files/home/\$_subpath\"")
            appendLine("            elif [ -n \"\$_subpath\" ] && [ -d \"/sdcard/\$_subpath\" ]; then")
            appendLine("                _target=\"/sdcard/\$_subpath\"")
            appendLine("            elif [ -n \"\$_subpath\" ] && [ -d \"/storage/emulated/0/\$_subpath\" ]; then")
            appendLine("                _target=\"/storage/emulated/0/\$_subpath\"")
            appendLine("            elif [ -n \"\$_subpath\" ]; then")
            appendLine("                _target=\"/data/data/com.termux/files/home/\$_subpath\"")
            appendLine("            else")
            appendLine("                _target=\"/data/data/com.termux/files/home\"")
            appendLine("            fi")
            appendLine("            ;;")
            appendLine("    esac")
            appendLine("    if builtin cd \"\$_target\" 2>/dev/null; then")
            appendLine("        return 0")
            appendLine("    fi")
            appendLine("    if [ -d \"/data/data/com.termux/files/home/\$_target\" ]; then")
            appendLine("        builtin cd \"/data/data/com.termux/files/home/\$_target\"")
            appendLine("    elif [ -d \"/sdcard/\$_target\" ]; then")
            appendLine("        builtin cd \"/sdcard/\$_target\"")
            appendLine("    elif [ -d \"/storage/emulated/0/\$_target\" ]; then")
            appendLine("        builtin cd \"/storage/emulated/0/\$_target\"")
            appendLine("    elif [ -d \"/sdcard/Download/\$_target\" ]; then")
            appendLine("        builtin cd \"/sdcard/Download/\$_target\"")
            appendLine("    else")
            appendLine("        mkdir -p \"/data/data/com.termux/files/home/\$_target\" 2>/dev/null || mkdir -p \"/sdcard/\$_target\" 2>/dev/null || mkdir -p \"/storage/emulated/0/\$_target\" 2>/dev/null")
            appendLine("        if builtin cd \"/data/data/com.termux/files/home/\$_target\" 2>/dev/null; then")
            appendLine("            return 0")
            appendLine("        elif builtin cd \"/sdcard/\$_target\" 2>/dev/null; then")
            appendLine("            return 0")
            appendLine("        elif builtin cd \"/storage/emulated/0/\$_target\" 2>/dev/null; then")
            appendLine("            return 0")
            appendLine("        else")
            appendLine("            builtin cd \"\$_target\"")
            appendLine("        fi")
            appendLine("    fi")
            appendLine("}")
            appendLine("ffprobe() {")
            appendLine("    if command -v ffprobe >/dev/null 2>&1; then")
            appendLine("        command ffprobe \"\$@\"")
            appendLine("        return \$?")
            appendLine("    fi")
            appendLine("    for _p in \"$prefixPath/bin/ffprobe\" /data/data/com.termux/files/usr/bin/ffprobe /system/bin/ffprobe /vendor/bin/ffprobe /data/local/tmp/ffprobe; do")
            appendLine("        if [ -x \"\$_p\" ]; then")
            appendLine("            \"\$_p\" \"\$@\"")
            appendLine("            return \$?")
            appendLine("        fi")
            appendLine("    done")
            appendLine("    echo \"⚠️ [ffprobe] ไม่สามารถเรียกใช้ได้จาก In-App Sandbox (ตรวจสอบที่ Prefix: $prefixPath/bin)\" >&2")
            appendLine("    echo \"💡 กรุณากดปุ่ม 'รันผ่าน Termux' เพื่อใช้งาน FFprobe หรือกำหนด Prefix Path ในเมนูการตั้งค่า\" >&2")
            appendLine("    return 1")
            appendLine("}")
            appendLine("ffmpeg() {")
            appendLine("    if command -v ffmpeg >/dev/null 2>&1; then")
            appendLine("        command ffmpeg \"\$@\"")
            appendLine("        return \$?")
            appendLine("    fi")
            appendLine("    for _p in \"$prefixPath/bin/ffmpeg\" /data/data/com.termux/files/usr/bin/ffmpeg /system/bin/ffmpeg /vendor/bin/ffmpeg /data/local/tmp/ffmpeg; do")
            appendLine("        if [ -x \"\$_p\" ]; then")
            appendLine("            \"\$_p\" \"\$@\"")
            appendLine("            return \$?")
            appendLine("        fi")
            appendLine("    done")
            appendLine("    echo \"⚠️ [ffmpeg] ไม่สามารถเรียกใช้ได้จาก In-App Sandbox (ตรวจสอบที่ Prefix: $prefixPath/bin)\" >&2")
            appendLine("    echo \"💡 กรุณากดปุ่ม 'รันผ่าน Termux' เพื่อใช้งาน Video Filter หรือกำหนด Prefix Path ในเมนูการตั้งค่า\" >&2")
            appendLine("    return 1")
            appendLine("}")
            appendLine("# ================================================================")
        }

        val lines = originalContent.lines()
        return if (lines.isNotEmpty() && lines[0].startsWith("#!")) {
            // Keep original shebang on first line
            lines[0] + "\n" + compatLayer + lines.drop(1).joinToString("\n")
        } else {
            "#!/system/bin/sh\n" + compatLayer + originalContent
        }
    }
}
