# FolderToText.ps1
# Usage: .\FolderToText.ps1 -TargetFolder "C:\path\to\folder" -Key 12345 -OutputFile "output.txt"

param(
    [Parameter(Mandatory=$true)]
    [string]$TargetFolder,

    [Parameter(Mandatory=$true)]
    [int]$Key,

    [string]$OutputFile = "output.txt"
)

if (-not (Test-Path $TargetFolder)) {
    Write-Host "Error: Target folder not found: $TargetFolder" -ForegroundColor Red
    exit 1
}

function Xor-Bytes($bytes, $key) {
    $keyBytes = [System.Text.Encoding]::UTF8.GetBytes($key.ToString())
    $keyLen = $keyBytes.Length
    $result = New-Object byte[] $bytes.Length
    for ($i = 0; $i -lt $bytes.Length; $i++) {
        $result[$i] = $bytes[$i] -bxor $keyBytes[$i % $keyLen]
    }
    return $result
}

$zipPath = [System.IO.Path]::GetTempFileName() + ".zip"

try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::CreateFromDirectory($TargetFolder, $zipPath)

    $bytes = [System.IO.File]::ReadAllBytes($zipPath)
    $encrypted = Xor-Bytes $bytes $Key
    $base64 = [Convert]::ToBase64String($encrypted)

    $chunkSize = 100000
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($OutputFile)
    $ext = [System.IO.Path]::GetExtension($OutputFile)
    $dir = [System.IO.Path]::GetDirectoryName($OutputFile)
    if (-not $dir) { $dir = "." }

    $totalChunks = [math]::Ceiling($base64.Length / $chunkSize)

    for ($i = 0; $i -lt $totalChunks; $i++) {
        $chunk = $base64.Substring($i * $chunkSize, [math]::Min($chunkSize, $base64.Length - $i * $chunkSize))
        if ($i -eq 0) {
            $outPath = $OutputFile
        } else {
            $outPath = Join-Path $dir ("${baseName}_" + ($i + 1) + "$ext")
        }
        [System.IO.File]::WriteAllText($outPath, $chunk)
        Write-Host "Saved: $outPath"
    }

    Write-Host "Done. $totalChunks file(s) created."
}
finally {
    if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
}
