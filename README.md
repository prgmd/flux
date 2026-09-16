# Flux
ListenBrainz 기반 글로벌 음악 트렌드 분석 데이터 파이프라인

---

## Overview

Flux는 '변화', '흐름'을 의미하는 라틴어 Fluxus에서 유래했습니다.

MetaBrainz의 ListenBrainz에서 발생한 음악 청취 로그를 수집, 가공하여 글로벌 음악 소비 동향, 시기별 트렌드, 아티스트 메타데이터 연계 분석을 수행하는 배치 데이터 파이프라인입니다.

정제된 데이터 마트는 PostgreSQL과 FastAPI를 통해 서빙되며, 추후 AI/LLM 모델이 음악 시장 트렌드 리포트를 자동 생성하거나 추천 시스템의 학습 피처로 활용할 수 있는 구조를 목표로 합니다.

---

## Architecture

Delta Lake 기반 [Medallion Architecture](https://learn.microsoft.com/ko-kr/azure/databricks/lakehouse/medallion)로 3단계 정제 레이어를 둡니다.

| 레이어 | 테이블 | 내용 |
|---|---|---|
| **Bronze** | `raw_listens` | 원본 JSON 보존. 덤프 단위 `replaceWhere`로 멱등 적재 |
| **Silver** | `listens_normalized` | MBID 우선순위 해석, 클라이언트 식별, 중복 제거, 품질 격리 |
| | `invalid_listens` | 품질 규칙 위반 격리 테이블 |
| **Gold** | `dim_artist` | 아티스트 차원. SCD Type 2로 개명 이력 보존 |
| | `fact_listens` | 청취 팩트. 아티스트 단위로 펼친 스타 스키마 (적재 검증 중) |

파티션은 Silver, Gold 모두 `listened_month`입니다. 명세는 `listened_date`였으나 청취 날짜가 7,877개로 흩어져 조각의 99.5%가 10만 행 미만이 되는 것을 실측하고 달 단위로 바꿨습니다.

---

## 진행 상황

| 단계 | 내용 | 상태 |
|---|---|:-:|
| S0 | 개발 환경 (Docker Compose, Spark 클러스터, Airflow, PostgreSQL) | 완료 |
| S1 | 덤프 수급 (체크섬 검증, 이어받기, 매니페스트) | 완료 |
| S2 | Bronze 적재 | 완료 |
| S3 | Silver 정제와 SCD Type 2 | 완료 |
| S4 | Gold 마트와 스큐 실험 | 진행 중 |
| S5 | PostgreSQL 서빙과 FastAPI | 예정 |
| S6 | Airflow 통합과 운영 마감 | 예정 |

---

## 실측

2026-08-08 ~ 09-07 증분 덤프 31일치 기준입니다.

| 항목 | 값 |
|---|---:|
| Bronze 적재 | 155,713,649행 |
| Silver 정제 | 151,192,624행 |
| 중복 제거 | 4,521,025행 (2.90%) |
| 품질 격리 | 0행 |
| `dim_artist` | 2,976,554 아티스트 + placeholder 661 |
| 파티션 (달) | 260개 |

중복 제거로 지워진 행 중 MBID 보유는 36.26%지만 그 99.6%가 `LASTFM` 출처이고, ListenBrainz가 직접 해석한 `MAPPING` 출처는 한 건도 지워지지 않았습니다. 정렬의 첫 키가 출처 순위이므로 남은 행이 언제나 그 그룹에서 가장 좋은 출처를 갖습니다.

청취에 실제로 쓰인 아티스트는 311,929명으로 차원의 10.5%이며, 그중 661명(0.21%)이 차원에 없어 placeholder로 처리합니다. 청취 기록과 MusicBrainz 덤프의 시점이 다르기 때문입니다.

---

## 검증

각 단계마다 통과해야 할 게이트를 정하고 실패를 주입해 확인합니다.

- **ACID**: Bronze 적재 도중 컨테이너를 강제 종료해 고아 parquet 44개가 읽히지 않는 것을 확인
- **SCD2 불변식**: 현재 줄이 정확히 1개, 현재 줄이 없는 아티스트 0건, 유효 구간이 뒤집힌 줄 0건. 297만 행 전수
- **개명 시나리오**: 아티스트 개명을 주입해 이력 2행이 생기는 것을 확인하고 시점 조회로 복원
- **MERGE 원자성**: 병합 도중 강제 종료. 2단계 MERGE는 반쪽 상태를 남겨 **게이트를 통과하지 못했고**, 한 문장으로 합쳐 다시 확인
- **결정성**: 입력 순서를 순열로 바꿔도 중복 제거 결과가 같은 것을 확인
- **교차 대조**: `groupBy`와 윈도우 함수 두 경로로 센 중복 제거 결과가 일치

순수 로직 테스트 447개는 Spark 없이 호스트에서 돕니다. 판단(무엇이 바뀌면 새 버전인가, 어느 줄을 남기는가)을 pyspark를 부르지 않는 모듈로 분리했기 때문입니다.

---

## Tech

**구현됨**

- Delta Lake 기반 Medallion Architecture (Bronze, Silver, Gold)
- Spark Batch로 덤프 적재와 MusicBrainz 메타데이터 조인
- Delta Lake `MERGE INTO`로 SCD Type 2 이력 관리. 닫기와 열기를 한 문장으로 처리해 원자성 확보
- 시점 범위 조인으로 과거 청취를 당시 아티스트 이름에 연결
- Delta Lake time travel을 실험 후 복원과 백필 검증에 활용

**예정**

- Data Skew 병목 극복을 위한 Salting 기법 적용
- AQE 제어 4단계 비교 실험과 수치 실측
- PostgreSQL 서빙 마트와 FastAPI 기반 조회 API
- Airflow DAG 통합

**Phase 2 (MVP 범위 밖)**

- Kafka 기반 이벤트 수집. 정적 덤프 재생에는 명분이 약해 실시간 API 연동 시점으로 미룸
- Spark Structured Streaming 적재
