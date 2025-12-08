# Weather Data Pipeline & Visualization Project

본 프로젝트는 네이버 웹 사이트의 실시간 및 예보 날씨 데이터를 수집하여
Airflow 기반 데이터 파이프라인으로 처리하고,
Google Cloud Storage(GCS)에 저장한 뒤,
웹 지도를 통해 시각화하는 개인프로젝트입니다.


## 개요

- 기상청 API를 이용해 전국 주요 도시의 날씨 데이터를 주기적으로 수집

- Airflow DAG을 이용해 주기적 자동 수집 및 적재

- 수집된 데이터를 CSV, Parquet 형태로 가공

- Google Cloud Storage(GCS)에 Raw 데이터 저장

- 저장된 데이터를 기반으로 웹 지도에서 시각화


## 시스템 구성

- 데이터 수집: Python (requests, BeautifulSoup)
- 오케스트레이션: Apache Airflow (Docker)
- 저장소: Google Cloud Storage (GCS)
- 웹 시각화: Flask + Leaflet
- 인프라: AWS EC2, Docker
- DB: PostgreSQL

## 데이터 파이프라인 요약
```plaintext
[네이버 웹 사이트]
        |
        v
[Python 크롤링 (crawl_now, crawl_after)]
        |
        v
[Google Cloud Storage - Raw 영역 (now / after)]
        |
        v
[PostgreSQL 적재 (정제 및 적재)]
        |
        v
[Google Cloud Storage - 배치처리]
     (daily / monthly)
        |
        +------------------------------+
        |                              |
        v                              v
[웹 서버 API (Flask)]          [장기 저장 / 분석용]
        |
        v
[Leaflet 기반 웹 지도 시각화]

```
### 1. 데이터 수집 (Crawling)
기상청 Open API를 이용하여 전국 주요 도시의 현재 날씨 및 예보 데이터를 Python으로 수집합니다.
수집된 원본 데이터는 가공 없이 1차적으로 GCS의 raw/now, raw/after 영역에 저장됩니다.

### 2. 1차 저장 (Google Cloud Storage - Raw)
실시간 수집 데이터 및 예보 데이터는 원본 CSV 형태로 GCS에 시간 단위로 적재됩니다.
장애 발생 시에도 원본 데이터를 그대로 보존할 수 있도록 Raw 영역을 분리하여 운영합니다.

### 3. DB 적재 (PostgreSQL)
GCS에 저장된 Raw 데이터를 PostgreSQL로 로드하여 컬럼 정규화 및 타입 정제 작업을 수행합니다.
이후 웹 서비스 및 일별·월별 집계를 위한 기준 데이터로 활용됩니다.

### 4. 2차 가공 및 재적재 (Daily / Monthly)
PostgreSQL에 적재된 데이터를 기준으로 일별(daily), 월별(monthly) 집계 데이터를 생성합니다.
생성된 결과 파일은 다시 GCS에 CSV 형태로 저장됩니다.

### 5. 웹 시각화 연동
Flask 기반 API 서버에서 GCS 또는 DB의 데이터를 조회합니다.
Leaflet 기반 웹 지도에서 전국 날씨 정보와 현재 위치의 날씨를 시각적으로 확인할 수 있습니다.

### 6. 오케스트레이션 (Airflow)
위 전 과정은 Apache Airflow DAG으로 스케줄링되어 자동 실행됩니다.
실시간, 일간, 월간 파이프라인이 각각 분리되어 운영됩니다.


## 웹 시각화 화면
### 전국 주요 도시의 날씨, 미세먼지, 외출점수를 매긴 것과, 현위치의 실시간 날씨 정보를 지도 위에 시각화하였습니다.
![웹 메인 화면](https://github.com/user-attachments/assets/78e29174-b537-42ef-ba09-5b541d5c1cee)

### 지도 확대 시 더 많은 도시의 날씨 정보가 상세하게 표시됩니다.
![확대 화면](https://github.com/user-attachments/assets/05700133-5077-4aa4-a9ff-dfc7f88a3c87)

## Airflow DAG 운영 화면
### 날씨 데이터 수집 및 적재를 담당하는 DAG들이 일정 주기로 정상 실행되고 있는 화면입니다.
![airflow](https://github.com/user-attachments/assets/dc906934-3784-4d8b-aa05-73f0fcb98286)

## Google Cloud Storage 저장 결과
### Airflow를 통해 생성된 파일들이 GCS의 디렉토리에 각각의 시간에 맞게 저장됩니다.
![GCS](https://github.com/user-attachments/assets/242b0655-0f32-45d4-95e8-f642423c5692)


## 주의 사항
본 프로젝트의 데이터 수집은 공공 데이터 API가 아닌 웹 페이지 구조를 직접 파싱하는 방식으로 구현되어 있습니다.
이로 인해 대상 웹사이트의 HTML 구조가 변경될 경우, 크롤링 로직에 대한 수정이 필요할 수 있습니다.

추후 공공 데이터 API를 활용한 방식으로도 동일한 파이프라인의 재구현이 가능하다면 다시 리빌딩하여
보다 안정적인 수집 구조와 신뢰성 있는 데이터 품질을 확보하는 방향으로 개선해볼 계획입니다.
