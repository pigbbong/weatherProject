# Weather Data Pipeline & Visualization Project

본 프로젝트는 네이버 웹사이트 및 공공 API의 실시간·예보 날씨 데이터를 수집하여,
Apache Airflow 기반 배치 파이프라인으로 자동화하고,
Raw / Processed 레이어로 분리된 GCS Data Lake에 적재한 뒤,
Flask + Leaflet 기반 웹 지도를 통해 시각화하는 데이터 엔지니어링 프로젝트입니다.

데이터 수집 → 저장 → 배치 처리 → 시각화까지
엔드투엔드 파이프라인 설계를 목표로 합니다.


## 개요

- 공공데이터포털 기상청 API를 통해 전국 주요 도시의 날씨 데이터를 주기적으로 수집

- Airflow DAG을 이용해 주기적 자동 수집 및 적재

- 수집된 데이터를 CSV, Parquet 형태로 가공

- Google Cloud Storage(GCS)에 Raw 데이터 저장

- 저장된 데이터를 기반으로 웹 지도에서 시각화


## 시스템 구성

- 데이터 수집: Crawling
- 오케스트레이션: Apache Airflow (Docker)
- 저장소: Google Cloud Storage (GCS)
- 웹 시각화: Flask + Leaflet
- 인프라: AWS EC2, Docker
- DB: PostgreSQL

## 데이터 파이프라인 요약
```plaintext
[공공데이터포털 / 기상청 API]
                |
                v
        [Python Crawlers]
        - now (초단기 실황)
        - ultraShort (초단기 예보)
        - shortFcst (단기 예보)
                |
                v
   [Google Cloud Storage - Raw Zone]
   - raw/weather/now/
   - raw/weather/ultraShort/
   - raw/weather/shortFcst/
                |
                v
   [Apache Airflow (Docker on EC2)]
   - DAG 스케줄링
   - 실패 재시도 / 로그 관리
   - SSH Port Forwarding으로 UI 접근
                |
                +--------------------------------------+
                |                                      |
                v                                      v
      [Spark Batch Processing]            [PostgreSQL (Latest View)]
      - daily / monthly / yearly          - 최신 상태 기준 테이블
      - partition(year/month/day)         - TRUNCATE + INSERT
                |                                      |
                v                                      |
   [Google Cloud Storage - Processed Zone]             |
   - processed/weather/batch/                          |
       - daily                                         |
       - monthly                                       |
       - yearly                                        |
                |                                      |
                v                                      v
          [BigQuery]                         [View / Materialized View]
          (Analytical DW)                              |
          - External / Load Table                      |
          - Partition 기반 분석                        |
                |                                      |
                v                                      v
        [Looker Studio]                       [Redis Cache Layer]
        (BI Dashboard)                       - 최신 조회 결과 캐싱
                                               - API 응답 속도 개선
                                                    |
                                                    v
                                            [Flask API Server]
                                                    |
                                                    v
                                           [Nginx Reverse Proxy]
                                                    |
                                                    v
                                        [Leaflet Web Visualization]
```
### 1. 데이터 수집 (Crawling)
공공데이터포털 기상청 API를 이용하여 전국 주요 도시의 초단기 실황(now), 초단기 예보(ultraShort), 단기 예보(shortFcst) 데이터를 Python으로 수집합니다.
수집된 데이터는 가공 없이 시간 단위 원본 데이터로 Google Cloud Storage(GCS)의 Raw 영역에 저장됩니다.

### 2. 1차 저장 (Google Cloud Storage - Raw)
실시간 수집 데이터 및 예보 데이터는 원본 Parquet 형태로 GCS에 시간 단위로 적재됩니다.
장애 발생 시에도 원본 데이터를 그대로 보존할 수 있도록 Raw 영역을 분리하여 운영합니다.

### 3. DB 적재 (PostgreSQL)
GCS Raw 데이터를 PostgreSQL로 로드하여 컬럼 정규화 및 타입 정제 작업을 수행합니다.
웹 서비스 조회 성능을 위해 최신 상태 기준 테이블(View / Materialized View) 을 유지하며,
TRUNCATE + INSERT 방식으로 최신 데이터만 관리합니다.

### 4. 배치 처리 및 2차 저장 (Spark + GCS Processed Zone)
Apache Spark를 이용하여 Raw 데이터를 기반으로
일별(daily), 월별(monthly), 연별(yearly) 배치 데이터를 생성합니다.
대용량 분석 및 후속 DW 연계를 고려한 Processed Zone 구조로 설계되었습니다.

### 5. 분석용 데이터 웨어하우스 연계 (BigQuery)
Processed Zone의 Parquet 데이터를 BigQuery로 로드하거나 External Table 형태로 연동하여
날짜 파티션 기반 분석이 가능하도록 구성하였습니다.

해당 데이터는 Looker Studio와 연동되어 통계 및 추이 분석용 대시보드로 활용됩니다.

### 6. 캐싱 및 웹 시각화 연동
PostgreSQL의 View / Materialized View를 기준으로 최신 날씨 데이터를 조회하고,
조회 결과는 Redis에 캐싱하여 반복 요청 시 DB 부하를 최소화합니다.

Flask 기반 API 서버는 Redis에 캐시된 데이터를 우선적으로 응답하며,
Leaflet 기반 웹 지도에서는 전국 날씨 정보와 사용자의 현재 위치 날씨를
빠르고 안정적으로 시각화하여 제공합니다.

### 7. 오케스트레이션 및 인프라 (Airflow, Docker, EC2)
전체 파이프라인은 Apache Airflow(Docker 기반) 로 구성되어 있으며,
AWS EC2 환경에서 운영됩니다.


## 웹 시각화 화면
### 전국 주요 도시의 날씨, 미세먼지, 외출점수를 매긴 것과, 현위치의 실시간 날씨 정보를 지도 위에 시각화하였습니다.
![웹 메인 화면](https://github.com/user-attachments/assets/7ed95969-80bb-4673-ba64-1ee132796964)

### 지도 확대 시 더 많은 도시의 날씨 정보가 상세하게 표시됩니다.
![확대 화면](https://github.com/user-attachments/assets/05700133-5077-4aa4-a9ff-dfc7f88a3c87)

## Airflow DAG 운영 화면
### 날씨 데이터 수집 및 적재를 담당하는 DAG들이 일정 주기로 정상 실행되고 있는 화면입니다.
![airflow](https://github.com/user-attachments/assets/52673dc9-1d31-486a-b44e-feb531cafbf4)

## Google Cloud Storage 저장 결과
### Airflow를 통해 생성된 파일들이 GCS의 디렉토리에 각각의 시간에 맞게 저장됩니다.
![GCS](https://github.com/user-attachments/assets/242b0655-0f32-45d4-95e8-f642423c5692)


## 참고 사항
- 직접 실행시킨다면, 코드 부분에서 AMI Key, GCS Bucket 이름, DB 비밀번호 등을 직접 설정한 후 실행해야합니다.

- 기존 네이버 웹 사이트를 직접 크롤링하는 방식에서 공공데이터포털API를 통해 크롤링 하는 방법으로 전환하였습니다. 기존의 방식은 branch의 old-main 부분을 참고하시면 됩니다.

- 추후 파이프라인 구조를 변경할 수 있습니다.
