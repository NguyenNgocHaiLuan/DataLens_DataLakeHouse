@echo off
REM Script to verify and create MinIO warehouse bucket if needed

echo Checking if 'warehouse' bucket exists in MinIO...

REM Install MinIO client if not already installed
REM docker run --rm --network vnjobs_api_datalakehouse_data-network ^
REM   --entrypoint /bin/sh minio/mc -c "mc alias set myminio http://minio:9000 minio_admin minio_password && mc ls myminio"

echo.
echo Creating 'warehouse' bucket in MinIO...

docker exec minio_jobs mc alias set myminio http://localhost:9000 minio_admin minio_password

docker exec minio_jobs mc mb myminio/warehouse --ignore-existing

docker exec minio_jobs mc ls myminio/

echo.
echo ✓ MinIO warehouse bucket is ready!
