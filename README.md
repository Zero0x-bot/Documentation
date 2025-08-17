# Documentation

Zero0x Trading Platform
Zero0x is a high-performance observability platform for Solana and Ethereum trading analytics, leveraging Axiom's event-driven architecture for scalable ingestion, storage, and querying of trade traces and metrics. This README provides an overview of features, setup instructions, and API usage for developers.
Features

Axiom Cloud Deployment: Fully managed infrastructure for seamless operation.
EventDB:
Ingest: Coordination-free pipeline with linear scaling, no Kafka required.
Storage: Custom block-based format with 25×-50× compression on object storage (S3).
Query: Serverless ephemeral runtimes using Axiom Processing Language (APL) for filtering, aggregations, and virtual fields.


Console:
Query Builders: Simple and advanced interfaces for APL-based queries.
Visualizations: Charts, graphs, and trace waterfalls for trade analysis.
Dashboards: Combine charts, log streams, and annotations for real-time insights.
Monitors: Threshold, match, and anomaly monitors for trade alerts.
Alerting: Webhooks, Slack, email, and custom integrations.


Governance:
Role-Based Access Control (RBAC) at dataset/organization levels.
Audit logs for tracking user actions.
Dataset management with retention and field vacuuming.


Integrations:
OpenTelemetry, Vector, Cribl, and AWS connectors (CloudWatch, Lambda, S3).
SDKs: Go, Python, Node.js, Java, Ruby, Rust, .NET.


APIs and CLI:
REST API for ingestion, querying, and management with API/personal tokens.
CLI for dataset creation, token management, and APL queries.
Terraform provider for resource management.


Security & Compliance: SOC 2 Type II, GDPR, CCPA, HIPAA compliant.

Architecture
Zero0x uses Axiom's distributed architecture for cost-efficient, scalable observability:

Ingestion: Regional edge proxies handle JSON/CSV data, with write-ahead logging for durability.
Storage: Columnar format with dictionary, bitmap, and numeric compression, stored in S3.
Query: Serverless workers execute APL queries with block-level parallelism and caching.
Compaction: Background optimization merges blocks for better compression and query speed.
Microservices: Stateless core services, database layer, and edge services ensure fault tolerance.

Setup Instructions
Prerequisites

MongoDB (libmongoc-dev, libjson-c-dev for C files)
PowerShell 7+ (Windows/Linux/macOS)
Git, GCC, and dependencies

Installation

Clone the Repository:
git clone https://github.com/your-org/zero0x.git
cd zero0x


Install Dependencies (Ubuntu/Debian):
sudo apt-get update
sudo apt-get install -y libmongoc-dev libjson-c-dev gcc


Compile C Files:
gcc -o trace_schema_doc trace_schema_doc.c -lmongoc-1.0 -lbson-1.0 -ljson-c
gcc -o query_usage_doc query_usage_doc.c -lmongoc-1.0 -lbson-1.0 -ljson-c
gcc -o system_requirements_doc system_requirements_doc.c -lmongoc-1.0 -lbson-1.0 -ljson-c
gcc -o region_trace_dispatcher region_trace_dispatcher.c -lmongoc-1.0 -lbson-1.0
gcc -o requirement_validator requirement_validator.c -lmongoc-1.0 -lbson-1.0


Set Environment Variables:
$env:MONGO_URI = "mongodb://localhost:27017"
[Environment]::SetEnvironmentVariable("MONGO_URI", "mongodb://localhost:27017", "User")



Usage

Generate Trace Schema Documentation:
.\trace_schema_doc.exe | Out-File -FilePath trace_schema_doc.json -Encoding utf8


Generate Query Usage Documentation:
Start-Process -FilePath ".\query_usage_doc.exe" -ArgumentList "org123" -RedirectStandardOutput "query_usage_doc.json" -Wait


Generate System Requirements Documentation:
.\system_requirements_doc.exe; if ($LASTEXITCODE -eq 0) { Get-Content system_requirements_doc.json | ConvertFrom-Json | Format-Table }


Dispatch Trace to Region:
$trace = '{"attributes":{"trade_id":"123","trade_type":"arbitrage"}}'
.\region_trace_dispatcher.exe US $trace > dispatcher_output.txt


Validate Trace Requirements:
$trace = '{"attributes":{"trade_id":"123","level":"info","trade_type":"arbitrage"},"_time":1697059200000}'
.\requirement_validator.exe $trace; if ($LASTEXITCODE -ne 0) { Write-Error "Validation failed" }


Run All Documentation Generators in Parallel:
$jobs = @(
    Start-Job -ScriptBlock { .\trace_schema_doc.exe },
    Start-Job -ScriptBlock { .\query_usage_doc.exe org123 },
    Start-Job -ScriptBlock { .\system_requirements_doc.exe }
)
$jobs | Wait-Job | Receive-Job
$jobs | Remove-Job



API Usage
REST API

Base URL: https://api.zero0x.trade/v1 (US), https://api.eu.zero0x.trade/v1 (EU)
Authentication: Use API tokens or Personal Access Tokens.
Endpoints:
POST /ingest: Send trade traces (JSON/CSV).$headers = @{ "Authorization" = "Bearer $env:API_TOKEN" }
$body = '{"attributes":{"trade_id":"123","level":"info"},"_time":"2025-08-17T10:31:00Z"}'
Invoke-RestMethod -Uri "https://api.zero0x.trade/v1/ingest" -Method Post -Headers $headers -Body $body -ContentType "application/json"


POST /query: Run APL queries.$query = "['trades_dataset'] | where attributes.trade_type=='arbitrage' | summarize count by chain_id"
$body = @{ "query" = $query } | ConvertTo-Json
Invoke-RestMethod -Uri "https://api.zero0x.trade/v1/query" -Method Post -Headers $headers -Body $body -ContentType "application/json"


GET /datasets: List datasets.Invoke-RestMethod -Uri "https://api.zero0x.trade/v1/datasets" -Method Get -Headers $headers





CLI

Create Dataset:axiom-cli dataset create trades_dataset --token $env:API_TOKEN --url https://api.zero0x.trade/v1


Run Query:axiom-cli query run --query "['trades_dataset'] | where chain_id=='solana'" --token $env:API_TOKEN --output json | ConvertFrom-Json



Development

Add New Feature:
git checkout -b feature/new-trace-endpoint
# Edit files, e.g., trace_schema_doc.c
git add .; git commit -m "Add new trace endpoint"; git push origin feature/new-trace-endpoint


Run Tests with MongoDB:
$env:TEST_MONGO_URI = "mongodb://localhost:27017/test"
gcc -o test_suite test_suite.c -lmongoc-1.0 -lbson-1.0 -ljson-c
.\test_suite.exe | Tee-Object -FilePath test_results.log



Contributing

Fork the repository and submit pull requests.
Follow code style: 4-space indentation, descriptive variable names.
Test changes with MongoDB locally before submitting.

License
MIT License. See LICENSE for details.
