param(
    [string]$Namespace = "self-healing-etl",
    [string]$PostgresPassword = "local-demo-password",
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "$Name is not available on PATH."
    }
}

function Invoke-Step {
    param(
        [string]$Message,
        [scriptblock]$Command
    )
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
    & $Command
}

Assert-Command docker
Assert-Command kubectl

Invoke-Step "Checking Docker daemon" {
    docker version | Out-Null
}

Invoke-Step "Checking kubectl cluster access" {
    kubectl cluster-info | Out-Null
}

Invoke-Step "Building ETL image" {
    docker build -f deployment/Dockerfile -t self-healing-etl:latest .
}

Invoke-Step "Building AI-SRE image" {
    docker build -f deployment/Dockerfile.aisre -t self-healing-etl-ai-sre:latest .
}

Invoke-Step "Applying namespace" {
    kubectl apply -f deployment/k8s/namespace.yaml
}

Invoke-Step "Applying local demo secret" {
    kubectl create secret generic self-healing-etl-secrets `
        --namespace $Namespace `
        --from-literal POSTGRES_PASSWORD=$PostgresPassword `
        --from-literal SLACK_WEBHOOK_URL="" `
        --dry-run=client -o yaml | kubectl apply -f -
}

Invoke-Step "Applying manifests" {
    kubectl apply -f deployment/k8s/configmap.yaml
    kubectl apply -f deployment/k8s/postgres.yaml
    kubectl apply -f deployment/k8s/etl-deployment.yaml
    kubectl apply -f deployment/k8s/ai-sre-deployment.yaml
}

Invoke-Step "Restarting deployments to pick up local image rebuilds" {
    kubectl rollout restart deployment/self-healing-etl -n $Namespace
    kubectl rollout restart deployment/ai-sre -n $Namespace
}

Invoke-Step "Waiting for PostgreSQL" {
    kubectl rollout status statefulset/postgres -n $Namespace --timeout="${TimeoutSeconds}s"
}

Invoke-Step "Waiting for ETL deployment" {
    kubectl rollout status deployment/self-healing-etl -n $Namespace --timeout="${TimeoutSeconds}s"
}

Invoke-Step "Waiting for AI-SRE deployment" {
    kubectl rollout status deployment/ai-sre -n $Namespace --timeout="${TimeoutSeconds}s"
}

Write-Host ""
Write-Host "Local Kubernetes demo is ready." -ForegroundColor Green
Write-Host "Run: python demo_k8s.py --scenario image_pull"
