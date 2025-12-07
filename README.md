# Weather Data Pipeline & Visualization Project

본 프로젝트는 기상청 Open API 기반의 실시간 및 예보 날씨 데이터를 수집하여
Airflow 기반 데이터 파이프라인으로 처리하고,
Google Cloud Storage(GCS)에 저장한 뒤,
웹 지도를 통해 시각화하는 개인 데이터 엔지니어링 프로젝트입니다.


## 개요

- 기상청 API를 이용해 전국 주요 도시의 날씨 데이터를 주기적으로 수집

- Airflow DAG을 이용해 주기적 자동 수집 및 적재

- 수집된 데이터를 CSV, Parquet 형태로 가공

- Google Cloud Storage(GCS)에 Raw 데이터 저장

- 저장된 데이터를 기반으로 웹 지도에서 시각화


## 시스템 구성

- 데이터 수집: Python (requests)
- 오케스트레이션: Apache Airflow (Docker)
- 저장소: Google Cloud Storage (GCS)
- 웹 시각화: Flask + Leaflet
- 인프라: AWS EC2, Docker


## 웹 시각화 화면
전국 주요 도시의 현재 날씨 및 예보 정보를 지도 위에 시각화하여 확인할 수 있습니다.
![웹 메인 화면](https://github.com/user-attachments/assets/78e29174-b537-42ef-ba09-5b541d5c1cee)

지도 확대 시 더 많은 도시의 날씨 정보가 상세하게 표시됩니다.


## Airflow DAG 운영 화면
날씨 데이터 수집 및 적재를 담당하는 DAG들이 일정 주기로 정상 실행되고 있는 화면입니다.


## Google Cloud Storage 저장 결과
Airflow를 통해 생성된 파일들이
GCS의 디렉토리에 각각의 시간에 맞게 저장됩니다.

