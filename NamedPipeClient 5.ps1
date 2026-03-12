#Requires -Version 5.1
<#
.SYNOPSIS
    名前付きパイプクライアント — NamedPipeServer.ps1 のテスト用クライアント

.使い方
    .\NamedPipeClient.ps1
    ※ NamedPipeServer.ps1 -Action Start を先に実行してください

.コマンド
    PING                              — サーバー疎通確認
    TIME                              — サーバー時刻の取得
    DBPING                            — Oracle接続テスト
    DBSTATUS                          — Oracle DB情報の表示
    DBQUERY SELECT * FROM V$VERSION   — SELECT実行
    DBEXEC  UPDATE employees SET salary=5000 WHERE id=1
    DBHELP                            — コマンド一覧の表示
    EXIT                              — 切断
#>

$PipeName   = "MyPipeServer"
$ServerName = "."    # "." = ローカルホスト

Write-Host ""
Write-Host "\\$ServerName\pipe\$PipeName に接続しています..." -ForegroundColor Cyan

$PipeClient = [System.IO.Pipes.NamedPipeClientStream]::new(
    $ServerName,
    $PipeName,
    [System.IO.Pipes.PipeDirection]::InOut
)

try {
    $PipeClient.Connect(5000)   # 5秒タイムアウト
    Write-Host "接続しました！コマンド一覧は DBHELP を入力してください。`n" -ForegroundColor Green

    $reader = [System.IO.StreamReader]::new($PipeClient)
    $writer = [System.IO.StreamWriter]::new($PipeClient)
    $writer.AutoFlush = $true

    # サーバーからのウェルカムメッセージを受信
    Write-Host "サーバー: $($reader.ReadLine())" -ForegroundColor Yellow
    Write-Host ""

    # 複数行レスポンス（<<END>>まで）を受信するヘルパー
    function Read-Response {
        param($r)
        $lines = @()
        while ($true) {
            $line = $r.ReadLine()
            if ($null -eq $line -or $line -eq "<<END>>") { break }
            $lines += $line
        }
        return $lines
    }

    do {
        $input = Read-Host "入力"
        if ([string]::IsNullOrWhiteSpace($input)) { continue }

        $writer.WriteLine($input)

        $cmd = ($input -split '\s+')[0].ToUpper()

        if ($cmd -ne "EXIT") {
            $lines = Read-Response -r $reader
            foreach ($l in $lines) {
                Write-Host "  $l" -ForegroundColor Yellow
            }
            Write-Host ""
        }
        else {
            $bye = $reader.ReadLine()
            Write-Host "  $bye" -ForegroundColor Yellow
        }
    } while ($cmd -ne "EXIT")
}
catch {
    Write-Host "エラー: $_" -ForegroundColor Red
}
finally {
    $PipeClient.Dispose()
    Write-Host "切断しました。" -ForegroundColor DarkCyan
}
