# 현재 아키텍처

> 2026-10-19 최신 주 출력은 사용자 이름+MSID+UTC 월별 Silver와 메타데이터 사전 및 격리 표다. 현재 검수 범위의 월별 및 연간 Gold는 DuckDB로 계산한 별도 Parquet 스냅샷이다. [최신 Gold 계약](monthly_gold.md)을 따른다. 아래 그림과 상세 흐름은 2026-10-07의 이전 모델이며, 최신 완료 범위와 다음 작업은 [현재 상태](status.md)를 따른다.

2026-10-07 저장소 코드 기준. 실행 중인 서비스의 상태를 조사한 문서는 아니다.

![현재 아키텍처](assets/architecture.png)

## 최신 월별 경로

```mermaid
flowchart LR
    B[Bronze Delta] --> S[월별 Silver Delta]
    M[MusicBrainz 공식 크레딧] --> R[곡 식별과 귀속 검수]
    S --> R
    S --> G[DuckDB 기간별 집계]
    R --> G
    G --> P[월별 및 연간 Gold Parquet]
    P --> V[조회용 DuckDB 뷰]
```

운영 원본은 보존하며 완료 manifest와 검수 결과가 있는 새 스냅샷을 읽는다. 미확정 귀속과 버전 분류는 결과의 상태 및 커버리지에 남긴다.

## 기존 상세 경로


1. `src/flux/ingest/`에서 덤프 목록과 체크섬을 확인하고 다운로드와 이어받기를 처리한다.
2. `src/flux/bronze/ingest.py`에서 청취 원본을 `raw_listens`에 보존한다.
3. `src/flux/silver/build.py`와 Silver 판단 모듈에서 MBID 해석, 중복 제거, 품질 검사를 수행한다. 유효 행은 `listens_normalized`, 불량 행은 `invalid_listens`로 나뉜다.
4. MusicBrainz 아티스트는 `src/flux/silver/musicbrainz.py`를 거쳐 `src/flux/gold/dim_artist.py`에서 SCD Type 2 차원이 된다. 청취마다 아티스트를 펼친 팩트는 `fact_listens`에 저장한다.
5. `dim_date.py`는 청취 기간의 UTC 날짜와 ISO 주를, `dim_user.py`는 Silver의 사용자 번호별 정보를 만든다.
6. `mart_build.py`는 일별 청취 마트와 주간 급상승 마트를 만든다. 급상승은 날짜 차원의 주 시작일로 묶고 지난주 청취 20회 이상인 아티스트 중 증가 비율 상위 20을 고른다.

그림의 화살표는 논리적 데이터 의존 관계다. 네트워크 연결도나 정확한 잡 실행 순서를 뜻하지 않는다. MusicBrainz는 청취 Bronze를 거치지 않는 별도 경로다.

## 실행 환경과 운영 기록

`scripts/pipeline/`의 CLI 잡이 Spark 세션을 사용한다. `docker-compose.yml`은 master 하나와 worker 세 개를 선언하고 공유 파일 경로를 워커에 마운트한다. 로컬 모드 실행도 지원한다. 분석 테이블은 Delta Lake 파일과 트랜잭션 로그로 저장한다.

`src/flux/common/job_runs.py`는 Spark 파이프라인 잡의 성공과 실패를 `ops/job_runs`에 남긴다. 기록 실패가 원래 잡의 성공이나 실패를 바꾸지 않도록 분리한다. Spark 이벤트 로그와 측정 보고서는 실행 시간, 셔플과 메모리를 조사하는 근거다.

## 아직 연결하지 않은 부분

- PostgreSQL 서빙 인스턴스는 Compose에 선언돼 있다. Gold 마트 내보내기와 FastAPI 조회 API는 후속 작업이다.
- Airflow 인프라와 `airflow/dags/flux_stack_smoke.py`는 있다. 데이터 파이프라인 DAG 통합은 후속 작업이다.
- Kafka와 Structured Streaming은 Phase 2 범위다.

## 이미지 수정

`assets/build_architecture.py`가 편집 가능한 원본이고 `assets/architecture.png`가 README에서 쓰는 결과물이다. Pillow가 설치된 Python에서 실행한다.

```powershell
python docs/assets/build_architecture.py
```

기본 폰트는 Windows의 Segoe UI다. 다른 운영체제에서는 스크립트의 폰트 경로를 설치된 폰트로 바꾼다. 구조를 바꿀 때는 코드의 테이블 이름과 Compose, DAG 구현 상태를 먼저 확인하고 그림과 이 문서를 함께 갱신한다.

`flux-hero.png`와 투명 배경 `flux-logo.png`는 Flux용으로 생성한 브랜딩 이미지다. 실제 실행 화면이나 데이터 측정 그래프를 뜻하지 않는다.
