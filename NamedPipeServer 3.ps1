#Requires -Version 5.1
<#
.SYNOPSIS
    名前付きパイプサーバー（RunspacePool + Oracle Managed Data Access）

.DESCRIPTION
    RunspacePool（最大10スレッド）を使用して、バックグラウンドプロセスとして
    名前付きパイプサーバー（\\.\pipe\MyPipeServer）を管理します。
    各クライアントセッションからOracle DBコマンドを実行できます。

.PARAMETER Action
    Start  — サーバーを隠しバックグラウンドプロセスとして起動します。
    Stop   — 実行中のサーバーを停止します。
    Status — サーバーの状態・稼働時間・Oracle DB接続状況を表示します。

.EXAMPLE
    .\NamedPipeServer.ps1 -Action Start
    .\NamedPipeServer.ps1 -Action Status
    .\NamedPipeServer.ps1 -Action Stop

.パイプクライアントコマンド
    PING                      — 疎通確認
    TIME                      — サーバー時刻の取得
    DBPING                    — Oracle接続テスト
    DBSTATUS                  — Oracle DBバージョン・セッション情報の表示
    DBQUERY <SELECT sql>      — SELECT実行（JSON形式で返却）
    DBEXEC  <DML sql>         — INSERT / UPDATE / DELETE 実行
    DBHELP                    — 利用可能なコマンド一覧の表示
    EXIT                      — 切断

.NOTES
    パイプ名      : MyPipeServer
    プール数      : 10 runspaces
    状態ファイル  : $env:TEMP\MyPipeServer.state.json
    ログファイル  : $env:TEMP\MyPipeServer.log

    Oracleドライバー:
      Oracle.ManagedDataAccess.dll（ODP.NET管理ドライバー）が必要です。
      以下の $OracleDllPath にOracleクライアントのインストールパスを設定してください。
      例: C:\oracle\product\<ver>\client_1\odp.net\managed\common\Oracle.ManagedDataAccess.dll
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [ValidateSet("Start", "Stop", "Status", "_Run")]
    [string] $Action
)

# ─────────────────────────────────────────────────────────────────────────────
# SHARED CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
$PipeName      = "MyPipeServer"
$MaxRunspaces  = 10
$StateFile     = "$env:TEMP\MyPipeServer.state.json"
$LogFile       = "$env:TEMP\MyPipeServer.log"
$StopEventName = "Global\MyPipeServer_Stop"   # Named EventWaitHandle for graceful shutdown
$GraceTimeout  = 15                           # seconds to wait before force kill

# ── Oracle設定 ────────────────────────────────────────────────────────────────
# OracleクライアントインストールのOracle.ManagedDataAccess.dllへのパスを指定します。
# 標準的なODAC / Oracleクライアントのパス例:
#   C:\oracle\product\<バージョン>\client_1\odp.net\managed\common\Oracle.ManagedDataAccess.dll
$OracleDllPath = "C:\oracle\product\12.2.0\client_1\odp.net\managed\common\Oracle.ManagedDataAccess.dll"

# Oracle接続文字列 — 以下の値を環境に合わせて変更してください
$OracleDataSource = "//localhost:1521/ORCL"   # ホスト:ポート/サービス名 または TNS別名
$OracleUserId     = "myuser"
$OraclePassword   = "mypassword"
$OracleConnString = "Data Source=$OracleDataSource;User Id=$OracleUserId;Password=$OraclePassword;"

# クエリタイムアウト（秒）
$OracleQueryTimeout = 30

# ─────────────────────────────────────────────────────────────────────────────
# HELPER — Write timestamped entry to log file
# ─────────────────────────────────────────────────────────────────────────────
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $entry = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Add-Content -Path $LogFile -Value $entry -Encoding UTF8
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPER — Load Oracle.ManagedDataAccess assembly (returns $true on success)
# ─────────────────────────────────────────────────────────────────────────────
function Import-OracleDriver {
    param([string]$DllPath)

    # Already loaded?
    if ([System.AppDomain]::CurrentDomain.GetAssemblies() |
        Where-Object { $_.GetName().Name -eq "Oracle.ManagedDataAccess" }) {
        return $true
    }

    # Load from Oracle Client path
    if (Test-Path $DllPath) {
        try {
            Add-Type -Path $DllPath -ErrorAction Stop
            return $true
        }
        catch {
            Write-Log "OracleドライバーDLLの読み込みに失敗しました ($DllPath): $_" "ERROR"
            return $false
        }
    }

    Write-Log "Oracle.ManagedDataAccess.dllが見つかりません: $DllPath" "ERROR"
    return $false
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPER — Read state file
# ─────────────────────────────────────────────────────────────────────────────
function Get-ServerState {
    if (Test-Path $StateFile) {
        try   { return Get-Content $StateFile -Raw | ConvertFrom-Json }
        catch { return $null }
    }
    return $null
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPER — Returns $true if the stored PID is a live powershell process
# ─────────────────────────────────────────────────────────────────────────────
function Test-ServerRunning {
    param($State)
    if ($null -eq $State) { return $false }
    try {
        $proc = Get-Process -Id $State.PID -ErrorAction Stop
        return ($proc.Name -match "powershell")
    }
    catch { return $false }
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPER — Quick Oracle connectivity test (used by Status action)
# ─────────────────────────────────────────────────────────────────────────────
function Test-OracleConnectivity {
    param([string]$ConnString, [string]$DllPath)

    if (-not (Import-OracleDriver -DllPath $DllPath)) {
        return @{ OK = $false; Message = "Oracleドライバーが読み込まれていません。" }
    }

    try {
        $conn = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($ConnString)
        $conn.Open()
        $ver = $conn.ServerVersion
        $conn.Close(); $conn.Dispose()
        return @{ OK = $true; Message = "接続成功。Oracleバージョン: $ver" }
    }
    catch {
        return @{ OK = $false; Message = "接続失敗: $_" }
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# ACTION: START
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-Start {
    $state = Get-ServerState

    if (Test-ServerRunning $state) {
        Write-Host "[サーバー] すでに起動中です (PID: $($state.PID))。" -ForegroundColor Yellow
        return
    }

    if (Test-Path $LogFile) { Remove-Item $LogFile -Force }

    Write-Host "[サーバー] 名前付きパイプサーバーを起動しています..." -ForegroundColor Cyan

    $proc = Start-Process -FilePath "powershell.exe" `
        -ArgumentList "-NonInteractive", "-ExecutionPolicy Bypass",
                      "-File `"$PSCommandPath`"", "-Action _Run" `
        -WindowStyle Hidden `
        -PassThru

    Start-Sleep -Milliseconds 800

    if ($null -ne $proc -and -not $proc.HasExited) {
        @{
            PID       = $proc.Id
            StartTime = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
            PipeName  = $PipeName
            PoolSize  = $MaxRunspaces
        } | ConvertTo-Json | Set-Content $StateFile -Encoding UTF8

        Write-Host ""
        Write-Host "======================================" -ForegroundColor Green
        Write-Host "   名前付きパイプサーバー  起動完了   " -ForegroundColor Green
        Write-Host "======================================" -ForegroundColor Green
        Write-Host "  PID        : $($proc.Id)"
        Write-Host "  パイプ     : \\.\pipe\$PipeName"
        Write-Host "  プール数   : $MaxRunspaces runspaces"
        Write-Host "  Oracle     : $OracleDataSource"
        Write-Host "  ログ       : $LogFile"
        Write-Host "======================================"
        Write-Host ""
    }
    else {
        Write-Host "[サーバー] 起動に失敗しました。ログを確認してください: $LogFile" -ForegroundColor Red
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# ACTION: STOP
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-Stop {
    $state = Get-ServerState

    if (-not (Test-ServerRunning $state)) {
        Write-Host "[サーバー] サーバーは起動していません。" -ForegroundColor Yellow
        if (Test-Path $StateFile) { Remove-Item $StateFile -Force }
        return
    }

    Write-Host "[サーバー] グレースフルシャットダウンを開始します (PID: $($state.PID))..." -ForegroundColor Cyan

    # ── Step 1: Signal the named event — server wakes instantly ───────────────
    try {
        $stopEvent = [System.Threading.EventWaitHandle]::OpenExisting($StopEventName)
        $stopEvent.Set()
        $stopEvent.Dispose()
        Write-Host "[サーバー] シャットダウンシグナルを送信しました。" -ForegroundColor Cyan
    }
    catch {
        Write-Host "[サーバー] 停止イベントへのアクセスに失敗しました: $_" -ForegroundColor Red
        Write-Host "[サーバー] サーバーはすでに停止している可能性があります。" -ForegroundColor Yellow
        Remove-Item $StateFile -Force -ErrorAction SilentlyContinue
        return
    }

    # ── Step 2: Wait for process to exit gracefully ────────────────────────────
    Write-Host "[サーバー] アクティブセッションの終了を待機中 (最大 ${GraceTimeout}秒)..." -ForegroundColor Yellow

    $proc    = Get-Process -Id $state.PID -ErrorAction SilentlyContinue
    $elapsed = 0
    while ($null -ne $proc -and -not $proc.HasExited -and $elapsed -lt $GraceTimeout) {
        Start-Sleep -Seconds 1
        $elapsed++
        Write-Host "  ... $elapsed 秒経過" -ForegroundColor DarkGray
        $proc = Get-Process -Id $state.PID -ErrorAction SilentlyContinue
    }

    # ── Step 3: Force kill only if graceful exit timed out ─────────────────────
    if ($null -ne $proc -and -not $proc.HasExited) {
        Write-Host "[サーバー] タイムアウト — プロセスを強制終了します。" -ForegroundColor Red
        try { Stop-Process -Id $state.PID -Force -ErrorAction Stop }
        catch { Write-Host "[サーバー] 強制終了エラー: $_" -ForegroundColor Red }
        Write-Log "グレースフルシャットダウンのタイムアウト後、強制終了しました。" "WARN"
    }
    else {
        Write-Host "[サーバー] サーバーが正常に停止しました。" -ForegroundColor Green
        Write-Log "サーバーがグレースフルシャットダウンで停止しました。" "INFO"
    }

    Remove-Item $StateFile -Force -ErrorAction SilentlyContinue
}

# ─────────────────────────────────────────────────────────────────────────────
# ACTION: STATUS
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-Status {
    $state = Get-ServerState

    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "   名前付きパイプサーバー — 状態確認    " -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    if (Test-ServerRunning $state) {
        $proc    = Get-Process -Id $state.PID -ErrorAction SilentlyContinue
        $started = [datetime]::ParseExact($state.StartTime, "yyyy-MM-dd HH:mm:ss", $null)
        $uptime  = (Get-Date) - $started

        Write-Host "  状態        : " -NoNewline; Write-Host "起動中" -ForegroundColor Green
        Write-Host "  PID         : $($state.PID)"
        Write-Host "  パイプ      : \\.\pipe\$($state.PipeName)"
        Write-Host "  プール数    : $($state.PoolSize) runspaces"
        Write-Host "  起動時刻    : $($state.StartTime)"
        Write-Host ("  稼働時間    : {0}日 {1}時間 {2}分 {3}秒" -f
            $uptime.Days, $uptime.Hours, $uptime.Minutes, $uptime.Seconds)

        if ($null -ne $proc) {
            Write-Host ("  CPU時間     : {0:N2} 秒"  -f $proc.CPU)
            Write-Host ("  メモリ使用  : {0:N1} MB" -f ($proc.WorkingSet64 / 1MB))
        }

        # Oracle connectivity check
        Write-Host ""
        Write-Host "  Oracle DB 接続確認:" -ForegroundColor DarkCyan
        $ora = Test-OracleConnectivity -ConnString $OracleConnString -DllPath $OracleDllPath
        if ($ora.OK) {
            Write-Host "    " -NoNewline
            Write-Host "OK  " -ForegroundColor Green -NoNewline
            Write-Host $ora.Message
        }
        else {
            Write-Host "    " -NoNewline
            Write-Host "失敗" -ForegroundColor Red -NoNewline
            Write-Host "  $($ora.Message)"
        }

        if (Test-Path $LogFile) {
            Write-Host ""
            Write-Host "  最近のログ（最新5行）:" -ForegroundColor DarkCyan
            Get-Content $LogFile -Tail 5 | ForEach-Object {
                Write-Host "    $_" -ForegroundColor DarkGray
            }
        }
    }
    else {
        Write-Host "  状態        : " -NoNewline; Write-Host "停止中" -ForegroundColor Red

        if ($null -ne $state) {
            Write-Host "  前回PID     : $($state.PID)"
            Write-Host "  前回起動    : $($state.StartTime)"
        }

        if (Test-Path $StateFile) { Remove-Item $StateFile -Force }
    }

    Write-Host ""
    Write-Host "  Oracle DS   : $OracleDataSource"
    Write-Host "  状態ファイル: $StateFile"
    Write-Host "  ログファイル: $LogFile"
    Write-Host "========================================"
    Write-Host ""
}

# ─────────────────────────────────────────────────────────────────────────────
# ACTION: _RUN  (internal — launched hidden by Start, do not call directly)
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-Run {

    Write-Log "サーバープロセスが起動しました。PID=$PID パイプ=$PipeName プール=$MaxRunspaces"

    # ── Oracle helper functions used inside runspace scriptblock ─────────────
    $OracleHelpers = {

        # Load Oracle driver inside the runspace
        function Load-OracleDriver {
            param([string]$DllPath)
            if ([System.AppDomain]::CurrentDomain.GetAssemblies() |
                Where-Object { $_.GetName().Name -eq "Oracle.ManagedDataAccess" }) {
                return $true
            }
            if (Test-Path $DllPath) {
                try { Add-Type -Path $DllPath -ErrorAction Stop; return $true }
                catch { return $false }
            }
            return $false
        }

        # ── DBPING — test connection ──────────────────────────────────────────
        function Invoke-DbPing {
            param([string]$ConnString, [string]$DllPath)
            if (-not (Load-OracleDriver -DllPath $DllPath)) {
                return "ERROR: Oracle driver not available."
            }
            try {
                $conn = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($ConnString)
                $conn.Open()
                $conn.Close(); $conn.Dispose()
                return "DBPONG — Oracle connection OK."
            }
            catch { return "ERROR: $($_.Exception.Message)" }
        }

        # ── DBSTATUS — server version + session info ──────────────────────────
        function Invoke-DbStatus {
            param([string]$ConnString, [string]$DllPath, [int]$Timeout)
            if (-not (Load-OracleDriver -DllPath $DllPath)) {
                return "ERROR: Oracle driver not available."
            }
            try {
                $conn = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($ConnString)
                $conn.Open()

                $sql = "SELECT SYS_CONTEXT('USERENV','DB_NAME')    AS DB_NAME,
                               SYS_CONTEXT('USERENV','SESSION_USER') AS SESSION_USER,
                               SYS_CONTEXT('USERENV','HOST')         AS HOST,
                               TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') AS DB_TIME
                        FROM DUAL"

                $cmd = $conn.CreateCommand()
                $cmd.CommandText    = $sql
                $cmd.CommandTimeout = $Timeout
                $reader = $cmd.ExecuteReader()
                $lines  = @("Oracle DB Status:", "  Version      : $($conn.ServerVersion)")

                if ($reader.Read()) {
                    $lines += "  DB Name      : $($reader['DB_NAME'])"
                    $lines += "  Session User : $($reader['SESSION_USER'])"
                    $lines += "  Client Host  : $($reader['HOST'])"
                    $lines += "  DB Time      : $($reader['DB_TIME'])"
                }

                $reader.Close(); $cmd.Dispose(); $conn.Close(); $conn.Dispose()
                return ($lines -join "`n")
            }
            catch { return "ERROR: $($_.Exception.Message)" }
        }

        # ── DBQUERY — execute SELECT, return JSON ─────────────────────────────
        function Invoke-DbQuery {
            param([string]$ConnString, [string]$DllPath, [int]$Timeout, [string]$Sql)

            if ([string]::IsNullOrWhiteSpace($Sql)) {
                return "ERROR: No SQL provided. Usage: DBQUERY <SELECT ...>"
            }

            # Safety: allow only SELECT / WITH
            $trimmed = $Sql.TrimStart()
            if ($trimmed -notmatch "^(SELECT|WITH)\s" ) {
                return "ERROR: DBQUERY only allows SELECT or WITH statements. Use DBEXEC for DML."
            }

            if (-not (Load-OracleDriver -DllPath $DllPath)) {
                return "ERROR: Oracle driver not available."
            }

            try {
                $conn = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($ConnString)
                $conn.Open()

                $cmd = $conn.CreateCommand()
                $cmd.CommandText    = $Sql
                $cmd.CommandTimeout = $Timeout

                $reader  = $cmd.ExecuteReader()
                $rows    = [System.Collections.Generic.List[hashtable]]::new()
                $maxRows = 500   # safety cap

                while ($reader.Read() -and $rows.Count -lt $maxRows) {
                    $row = [ordered]@{}
                    for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                        $col   = $reader.GetName($i)
                        $val   = if ($reader.IsDBNull($i)) { $null } else { $reader.GetValue($i).ToString() }
                        $row[$col] = $val
                    }
                    $rows.Add($row)
                }

                $reader.Close(); $cmd.Dispose(); $conn.Close(); $conn.Dispose()

                $result = @{
                    RowCount = $rows.Count
                    Rows     = $rows
                }
                return ($result | ConvertTo-Json -Depth 5 -Compress)
            }
            catch { return "ERROR: $($_.Exception.Message)" }
        }

        # ── DBEXEC — execute INSERT / UPDATE / DELETE / DDL ───────────────────
        function Invoke-DbExec {
            param([string]$ConnString, [string]$DllPath, [int]$Timeout, [string]$Sql)

            if ([string]::IsNullOrWhiteSpace($Sql)) {
                return "ERROR: No SQL provided. Usage: DBEXEC <INSERT|UPDATE|DELETE ...>"
            }

            # Block SELECT to avoid misuse
            $trimmed = $Sql.TrimStart()
            if ($trimmed -match "^SELECT\s") {
                return "ERROR: Use DBQUERY for SELECT statements."
            }

            if (-not (Load-OracleDriver -DllPath $DllPath)) {
                return "ERROR: Oracle driver not available."
            }

            try {
                $conn = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($ConnString)
                $conn.Open()

                $tx  = $conn.BeginTransaction()
                $cmd = $conn.CreateCommand()
                $cmd.Transaction  = $tx
                $cmd.CommandText  = $Sql
                $cmd.CommandTimeout = $Timeout

                $affected = $cmd.ExecuteNonQuery()
                $tx.Commit()

                $cmd.Dispose(); $conn.Close(); $conn.Dispose()
                return "OK: $affected row(s) affected."
            }
            catch {
                return "ERROR: $($_.Exception.Message)"
            }
        }

        # ── DBHELP ─────────────────────────────────────────────────────────────
        function Get-DbHelp {
            return @"
Oracle DB Commands:
  DBPING              — Test Oracle connection
  DBSTATUS            — Show DB name, session user, server time
  DBQUERY <sql>       — Run a SELECT (max 500 rows, returns JSON)
  DBEXEC  <sql>       — Run INSERT / UPDATE / DELETE (auto-commit)
  DBHELP              — Show this help

General Commands:
  PING                — Server health check
  TIME                — Server timestamp
  EXIT                — Disconnect
"@
        }
    }

    # ── Client handler scriptblock ────────────────────────────────────────────
    $ClientHandler = {
        param(
            [System.IO.Pipes.NamedPipeServerStream] $PipeStream,
            [int]    $ClientID,
            [string] $LogFile,
            [string] $ConnString,
            [string] $DllPath,
            [int]    $QueryTimeout
        )

        # ── Embed oracle helpers inside runspace ──────────────────────────────
        function Write-Log {
            param([string]$Message, [string]$Level = "INFO")
            $entry = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
            Add-Content -Path $LogFile -Value $entry -Encoding UTF8
        }

        function Load-OracleDriver {
            param([string]$DllPath)
            if ([System.AppDomain]::CurrentDomain.GetAssemblies() |
                Where-Object { $_.GetName().Name -eq "Oracle.ManagedDataAccess" }) { return $true }
            if (Test-Path $DllPath) {
                try { Add-Type -Path $DllPath -ErrorAction Stop; return $true }
                catch { return $false }
            }
            return $false
        }

        function Invoke-DbPing {
            param([string]$CS, [string]$Dll)
            if (-not (Load-OracleDriver $Dll)) { return "エラー: Oracleドライバーが利用できません。" }
            try {
                $c = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($CS)
                $c.Open(); $c.Close(); $c.Dispose()
                return "DBPONG — Oracle接続OK。"
            } catch { return "エラー: $($_.Exception.Message)" }
        }

        function Invoke-DbStatus {
            param([string]$CS, [string]$Dll, [int]$TO)
            if (-not (Load-OracleDriver $Dll)) { return "エラー: Oracleドライバーが利用できません。" }
            try {
                $c   = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($CS)
                $c.Open()
                $sql = "SELECT SYS_CONTEXT('USERENV','DB_NAME') AS DB_NAME,
                               SYS_CONTEXT('USERENV','SESSION_USER') AS SESSION_USER,
                               SYS_CONTEXT('USERENV','HOST') AS HOST,
                               TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') AS DB_TIME
                        FROM DUAL"
                $cmd = $c.CreateCommand(); $cmd.CommandText = $sql; $cmd.CommandTimeout = $TO
                $r   = $cmd.ExecuteReader()
                $lines = @("Oracle DB 状態:", "  バージョン        : $($c.ServerVersion)")
                if ($r.Read()) {
                    $lines += "  DB名              : $($r['DB_NAME'])"
                    $lines += "  セッションユーザー: $($r['SESSION_USER'])"
                    $lines += "  クライアントホスト: $($r['HOST'])"
                    $lines += "  DB時刻            : $($r['DB_TIME'])"
                }
                $r.Close(); $cmd.Dispose(); $c.Close(); $c.Dispose()
                return ($lines -join "`n")
            } catch { return "エラー: $($_.Exception.Message)" }
        }

        function Invoke-DbQuery {
            param([string]$CS, [string]$Dll, [int]$TO, [string]$Sql)
            if ([string]::IsNullOrWhiteSpace($Sql)) { return "エラー: SQLが指定されていません。使い方: DBQUERY <SELECT ...>" }
            if ($Sql.TrimStart() -notmatch "^(SELECT|WITH)\s") {
                return "エラー: DBQUERYはSELECT/WITHのみ使用できます。DMLはDBEXECを使用してください。"
            }
            if (-not (Load-OracleDriver $Dll)) { return "エラー: Oracleドライバーが利用できません。" }
            try {
                $c = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($CS); $c.Open()
                $cmd = $c.CreateCommand(); $cmd.CommandText = $Sql; $cmd.CommandTimeout = $TO
                $r = $cmd.ExecuteReader()
                $rows = [System.Collections.Generic.List[hashtable]]::new()
                while ($r.Read() -and $rows.Count -lt 500) {
                    $row = [ordered]@{}
                    for ($i = 0; $i -lt $r.FieldCount; $i++) {
                        $row[$r.GetName($i)] = if ($r.IsDBNull($i)) { $null } else { $r.GetValue($i).ToString() }
                    }
                    $rows.Add($row)
                }
                $r.Close(); $cmd.Dispose(); $c.Close(); $c.Dispose()
                return (@{ 件数 = $rows.Count; 行データ = $rows } | ConvertTo-Json -Depth 5 -Compress)
            } catch { return "エラー: $($_.Exception.Message)" }
        }

        function Invoke-DbExec {
            param([string]$CS, [string]$Dll, [int]$TO, [string]$Sql)
            if ([string]::IsNullOrWhiteSpace($Sql)) { return "エラー: SQLが指定されていません。使い方: DBEXEC <DML ...>" }
            if ($Sql.TrimStart() -match "^SELECT\s") { return "エラー: SELECTにはDBQUERYを使用してください。" }
            if (-not (Load-OracleDriver $Dll)) { return "エラー: Oracleドライバーが利用できません。" }
            try {
                $c   = [Oracle.ManagedDataAccess.Client.OracleConnection]::new($CS); $c.Open()
                $tx  = $c.BeginTransaction()
                $cmd = $c.CreateCommand()
                $cmd.Transaction = $tx; $cmd.CommandText = $Sql; $cmd.CommandTimeout = $TO
                $n   = $cmd.ExecuteNonQuery(); $tx.Commit()
                $cmd.Dispose(); $c.Close(); $c.Dispose()
                return "OK: $n 行が影響を受けました。"
            } catch { return "エラー: $($_.Exception.Message)" }
        }

        function Get-DbHelp {
            return @"
Oracle DBコマンド:
  DBPING              - Oracle接続テスト
  DBSTATUS            - DB名・セッションユーザー・サーバー時刻の表示
  DBQUERY <sql>       - SELECT実行（最大500行、JSON形式で返却）
  DBEXEC  <sql>       - INSERT / UPDATE / DELETE 実行（自動コミット）
  DBHELP              - このヘルプを表示

一般コマンド:
  PING                - サーバー疎通確認
  TIME                - サーバー時刻の取得
  EXIT                - 切断
"@
        }

        # ── Main client session ───────────────────────────────────────────────
        try {
            Write-Log "クライアント #$ClientID が接続しました。"

            $reader = [System.IO.StreamReader]::new($PipeStream)
            $writer = [System.IO.StreamWriter]::new($PipeStream)
            $writer.AutoFlush = $true

            $writer.WriteLine("名前付きパイプサーバーに接続しました。クライアント #$ClientID です。コマンド一覧は DBHELP を入力してください。")

            while ($PipeStream.IsConnected) {
                $line = $reader.ReadLine()
                if ($null -eq $line) { break }

                $trimLine = $line.Trim()
                Write-Log "Client #$ClientID >> $trimLine"

                # Parse command and optional argument
                $parts   = $trimLine -split '\s+', 2
                $cmd     = $parts[0].ToUpper()
                $cmdArgs = if ($parts.Count -gt 1) { $parts[1] } else { "" }

                $response = switch ($cmd) {
                    "EXIT"     { $writer.WriteLine("接続を切断します。クライアント #$ClientID さん、ありがとうございました。"); break }
                    "PING"     { "PONG" }
                    "TIME"     { "サーバー時刻: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" }
                    "DBPING"   { Invoke-DbPing   -CS $ConnString -Dll $DllPath }
                    "DBSTATUS" { Invoke-DbStatus -CS $ConnString -Dll $DllPath -TO $QueryTimeout }
                    "DBQUERY"  { Invoke-DbQuery  -CS $ConnString -Dll $DllPath -TO $QueryTimeout -Sql $cmdArgs }
                    "DBEXEC"   { Invoke-DbExec   -CS $ConnString -Dll $DllPath -TO $QueryTimeout -Sql $cmdArgs }
                    "DBHELP"   { Get-DbHelp }
                    default    { "不明なコマンドです: $cmd — コマンド一覧は DBHELP を入力してください。" }
                }

                if ($cmd -ne "EXIT" -and $null -ne $response) {
                    # Send multi-line responses line by line, then end marker
                    $response -split "`n" | ForEach-Object { $writer.WriteLine($_) }
                    $writer.WriteLine("<<END>>")   # signals end of multi-line response to client
                }

                if ($cmd -eq "EXIT") { break }
            }
        }
        catch {
            Write-Log "クライアント #$ClientID でエラーが発生しました: $_" "ERROR"
        }
        finally {
            if ($PipeStream.IsConnected) { $PipeStream.Disconnect() }
            $PipeStream.Dispose()
            Write-Log "クライアント #$ClientID が切断しました。"
        }
    }

    # ── Build RunspacePool ────────────────────────────────────────────────────
    $SessionState = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
    $RunspacePool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(
        1, $MaxRunspaces, $SessionState, $Host
    )
    $RunspacePool.Open()
    Write-Log "RunspacePoolを開きました (最大=$MaxRunspaces)。"

    $ActiveJobs = [System.Collections.Generic.List[hashtable]]::new()
    $ClientID   = 0

    # ── Create the named stop event (auto-reset, initially not signalled) ────────
    $stopEvent = [System.Threading.EventWaitHandle]::new(
        $false,                                                        # initially not signalled
        [System.Threading.EventResetMode]::ManualReset,
        $StopEventName
    )
    Write-Log "停止イベントを作成しました: $StopEventName"

    # ── Main accept loop ──────────────────────────────────────────────────────
    try {
        while ($true) {
            $PipeStream = [System.IO.Pipes.NamedPipeServerStream]::new(
                $PipeName,
                [System.IO.Pipes.PipeDirection]::InOut,
                $MaxRunspaces,
                [System.IO.Pipes.PipeTransmissionMode]::Message,
                [System.IO.Pipes.PipeOptions]::Asynchronous
            )

            Write-Log "クライアント接続を待機中..."

            # Begin async wait for a client connection
            $asyncConn = $PipeStream.BeginWaitForConnection($null, $null)

            # WaitAny: wake on client connect (index 0) OR stop event (index 1)
            $handles   = [System.Threading.WaitHandle[]] @(
                $asyncConn.AsyncWaitHandle,
                $stopEvent
            )
            $signalled = [System.Threading.WaitHandle]::WaitAny($handles)

            # Stop event was signalled — graceful shutdown
            if ($signalled -eq 1) {
                Write-Log "停止シグナルを受信しました。新規接続の受付を終了します。"
                $PipeStream.Dispose()
                break
            }

            # Client connected — complete the async accept
            $PipeStream.EndWaitForConnection($asyncConn)

            $ClientID++
            Write-Log "クライアント #$ClientID を受け付けました — runspaceに割り当て中。"

            $PS = [System.Management.Automation.PowerShell]::Create()
            $PS.RunspacePool = $RunspacePool
            [void]$PS.AddScript($ClientHandler)
            [void]$PS.AddParameter("PipeStream",    $PipeStream)
            [void]$PS.AddParameter("ClientID",      $ClientID)
            [void]$PS.AddParameter("LogFile",       $LogFile)
            [void]$PS.AddParameter("ConnString",    $OracleConnString)
            [void]$PS.AddParameter("DllPath",       $OracleDllPath)
            [void]$PS.AddParameter("QueryTimeout",  $OracleQueryTimeout)

            $AsyncResult = $PS.BeginInvoke()
            $ActiveJobs.Add(@{
                PS          = $PS
                AsyncResult = $AsyncResult
                ClientID    = $ClientID
            })

            # Purge completed jobs
            $done = $ActiveJobs | Where-Object { $_.AsyncResult.IsCompleted }
            foreach ($job in $done) {
                try   { $job.PS.EndInvoke($job.AsyncResult) } catch {}
                $job.PS.Dispose()
                $ActiveJobs.Remove($job)
            }
        }
    }
    catch {
        Write-Log "サーバーループが終了しました: $_" "ERROR"
    }
    finally {
        # Wait for all active client sessions to finish naturally
        if ($ActiveJobs.Count -gt 0) {
            Write-Log "残り $($ActiveJobs.Count) セッションの完了を待機中..."
            foreach ($job in $ActiveJobs) {
                try { $job.PS.EndInvoke($job.AsyncResult) } catch {}
                $job.PS.Dispose()
            }
        }
        $stopEvent.Dispose()
        $RunspacePool.Close()
        $RunspacePool.Dispose()
        Write-Log "RunspacePoolを解放しました。サーバープロセスを終了します。"
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
switch ($Action) {
    "Start"  { Invoke-Start  }
    "Stop"   { Invoke-Stop   }
    "Status" { Invoke-Status }
    "_Run"   { Invoke-Run    }
}
