$ErrorActionPreference = "Stop"

$dockerExe = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
if (-not (Test-Path -LiteralPath $dockerExe)) {
    $dockerExe = "docker"
}

$containerName = "quick_tunnel_live"
$networkName = "shop-network"
$targetUrl = "http://frontend-caddy-1:80"

# Remove old quick tunnel if it exists.
& $dockerExe rm -f $containerName *> $null

# Start a fresh quick tunnel container.
& $dockerExe run -d --name $containerName --network $networkName cloudflare/cloudflared:latest tunnel --protocol quic --edge-ip-version 4 --no-autoupdate --url $targetUrl | Out-Null

Start-Sleep -Seconds 6

$logs = & $dockerExe logs $containerName 2>&1
$matches = [regex]::Matches(($logs -join "`n"), "https://[a-z0-9-]+\.trycloudflare\.com")
if ($matches.Count -eq 0) {
    Write-Host "Could not extract trycloudflare URL from logs." -ForegroundColor Red
    Write-Host "Run: docker logs $containerName --tail 120"
    exit 1
}

$url = $matches[$matches.Count - 1].Value
Write-Host "Quick tunnel URL:" -ForegroundColor Cyan
Write-Host $url -ForegroundColor Green

Write-Host "\nHealth check:" -ForegroundColor Cyan
curl.exe -I $url
