param([string]$ComposePath = '')
$ErrorActionPreference = 'Stop'
$projectCollectionCompose = if ($ComposePath) { $ComposePath } else {
    Join-Path $env:USERPROFILE 'OneDrive\Documents\ChatGPT\Bigdata\project_runtime\airflow\compose.yaml'
}
if (-not (Test-Path -LiteralPath $projectCollectionCompose)) {
    throw "หาไฟล์ตั้งค่าระบบเก็บข้อมูลไม่พบ: $projectCollectionCompose"
}
docker desktop start
if ($LASTEXITCODE -ne 0) { throw 'เปิด Docker Desktop ไม่สำเร็จ' }
docker compose -f $projectCollectionCompose up -d project-db airflow-apiserver airflow-scheduler airflow-dag-processor
if ($LASTEXITCODE -ne 0) { throw 'เปิดระบบเก็บข้อมูลไม่สำเร็จ' }
foreach ($scrapingDag in @('bangkok_bus_gps','bangkok_live_mobility','bangkok_travel_times','bangkok_gtfs_refresh')) {
    docker compose -f $projectCollectionCompose exec -T airflow-scheduler airflow dags unpause $scrapingDag
    if ($LASTEXITCODE -ne 0) { throw "เปิดงาน $scrapingDag ไม่สำเร็จ" }
}
Write-Host 'ระบบเดิมเก็บต่อเนื่องแล้ว ไม่สร้าง collector ซ้ำ — http://localhost:8080'
Write-Host 'GPS สาย 1-21 (114) สองทิศทางทุก 1 นาที; จราจร/เหตุการณ์ทุก 15 นาที'
Write-Host 'ต้องเปิดเครื่องและ Docker ไว้; Longdo travel_times จะข้ามเมื่อไม่มี API key'
