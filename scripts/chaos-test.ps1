Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Test-NodeReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Port
    )

    for ($attempt = 1; $attempt -le 30; $attempt++) {
        try {
            $client = New-Object Net.Sockets.TcpClient
            $client.Connect('localhost', [int]$Port)
            $client.Close()
            return
        } catch {
            if ($client) {
                $client.Close()
            }
            Start-Sleep -Seconds 1
        }
    }

    throw "Timed out waiting for node on port $Port"
}

Write-Host "Starting distributed counter cluster for chaos tests..."
Invoke-Compose @('-f', 'deployments/docker-compose.yml', 'up', '-d', '--build')

try {
    Write-Host "Waiting for cluster startup..."
    Start-Sleep -Seconds 8

    foreach ($port in @('50051', '50052', '50053')) {
        Write-Host "Checking node on port $port..."
        Test-NodeReady -Port $port
    }

    Write-Host "Running docker chaos test package..."
    & go test -count=1 -tags chaosdocker -timeout 120s -v ./test/chaos/...
    if ($LASTEXITCODE -ne 0) {
        throw "go test failed with exit code $LASTEXITCODE"
    }
}
finally {
    Write-Host "Stopping distributed counter cluster..."
    Invoke-Compose @('-f', 'deployments/docker-compose.yml', 'down', '-v')
}