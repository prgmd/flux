# 전체 월별 Silver에서 dbt Gold 검증

## 입력과 운영 경계

[표본 이전](dbt_pilot.md)의 SQL을 전체 월별 Silver에 적용한다. P0가 기록한 네 Delta 입력의 버전과 ID, 커밋 지문 및 활성 파일의 크기와 수정 시각을 다시 확인한다. 기존 Gold와 곡 사전, 공식 귀속 및 검수 자료의 SHA-256도 확인한다. 월별 Silver는 사용자 이름+MSID+UTC 청취 월 80,563,985행이며 play_count 합은 157,351,363회다. 전체 입력과 비교할 기존 Gold는 같은 30초 정책과 귀속 정책을 사용해야 한다.

실행기는 이 월별 입력과 기존 Gold의 곡 사전 및 라벨과 공식 크레딧을 새 DuckDB의 pinned 스키마에 복사한다. dbt는 이 DB만 읽는다. Bronze와 Silver, 격리 및 CDF 설정, 기존 Gold Parquet와 이전 표본 출력은 교체하지 않는다. 새 곡 연결이나 귀속 승인을 추가하는 단계도 아니다. 원본과 출력이 서로 포함되는 경로는 거부한다.

## 전체 실행의 물리 구성

논리 모델 24개와 최종 마트 14개의 SQL은 표본과 같다. 실행 프로젝트 사본에서만 사용자별 중간 뷰 6개를 테이블로 바꾼다. 표본에서는 뷰의 재계산 비용이 작았지만 전체 실행에서는 같은 사용자별 기여를 여러 마트와 검사에서 재사용한다. staging 4개는 뷰, 중간 6개와 최종 14개는 테이블이다. 입력과 중간 결과를 보관하는 DB 용량을 최종 마트 파일 크기와 구분한다.

dbt 작업은 한 번에 하나씩 실행하며 DuckDB 계산 스레드는 2개, 최초 엔진 메모리 설정은 8GB였고, 전체 검사 복구와 최종 내보내기 및 비교는 16GB로 실행했다. 입력 복사 및 행 지문 준비는 8GB 설정을 유지한다. 임시 디스크 한도는 100GB다. 엔진 설정은 실제 프로세스의 최고 RAM 사용량과 같은 측정값이 아니다. Windows에서는 실행 중에만 시스템 절전을 방지하고 종료 시 해제한다. 전역 전원 설정을 바꾸지 않는다.

임시 경로는 어댑터의 config_options로 DB 연결 생성 시점에 설정한다. settings에 넣으면 커서를 열 때마다 SET을 실행한다. 외부 파일 접근을 차단한 뒤에는 temp_directory 변경이 거부되어 첫 연결 또는 후속 연결이 실패할 수 있다. 외부 접근 차단은 유지하고 설정 시점을 옮겼다. 이 구성은 별도 표본 통합 테스트에서 실제 두 build와 문서 생성 및 14개 결과 대조까지 확인한다.

## 실행과 전수 대조

최초 연결 확인 후 dbt build에서 모델 24개와 데이터 테스트 145개를 실행한다. 이번 전체 8GB 실행은 연간 기여의 EXCEPT ALL 대조에서 메모리 오류가 났다. 이를 키의 FULL JOIN과 횟수 대조로 바꾸고 키 유일성은 기존 전용 검사에서 유지했다. 전체 품질 계약은 중복 행도 계속 거부하며 실제 중복 주입으로 확인했다. 수정 SQL과 16GB를 함께 적용한 대조는 73.85초에 통과했다. 한 변경만의 개선 비율로 해석하지 않는다.

실패와 건너뛴 68개는 dbt의 result:error+, result:fail+ 및 result:skipped와 보존된 run_results 및 manifest로 선택해 재실행했다. 이전 성공 상태와 새 결과를 합쳐 169개 전부 성공 또는 통과여야 한다. 재시도 상태 파일의 지문도 마지막까지 확인한다. 이전 실행 프로젝트와 달라질 수 있는 파일은 프로파일과 해당 대조 검사뿐이며 모델 SQL이나 스키마 계약이 달라졌으면 성공 모델을 재사용하지 않는다. 모두 통과해야 문서 생성과 마트 내보내기 및 비교로 이어진다. 같은 전체 입력으로 두 번째 build를 자동 반복하지 않는다. 재실행 의미는 기존 표본에서 확인했고 전체 실행에서는 생성과 검사 및 기존 전체 결과 대조에 시간을 사용한다.

최종 마트는 기존 Gold와 같은 Parquet 및 ZSTD 압축, 같은 표시 정렬식으로 내보낸다. 비교는 Python 객체로 전체 행을 가져오지 않는다. 각 월 또는 연도에 해당하는 두 Parquet 범위를 SQL로 읽어 전체 키의 FULL JOIN을 수행한다. 키가 NULL이거나 중복이면 먼저 거부한다. 한쪽에만 있는 키와 값이 다른 컬럼을 각각 센다. 행 수와 총합만 같은 다른 결과도 실패한다.

컬럼 순서와 자료형을 확인하고 정수 및 문자열과 날짜, NULL은 그대로 비교한다. 정수를 실수로 바꿔 비교하지 않는다. 실수만 상대 및 절대 허용 오차 1e-12를 적용하고 NaN 및 무한대는 거부한다. 기간 목록도 양쪽의 합집합을 사용해 한쪽에만 있는 기간을 놓치지 않는다. 비교 결과는 표별 누락과 추가 행, 컬럼별 불일치 수 및 검사한 기간 수로 저장한다.

완료 전에는 복사 입력의 행 수와 행 해시 집계, 원래 Delta 버전 및 활성 파일 정보, 기존 자료의 SHA-256과 실행 프로젝트 지문을 다시 확인한다. 복사 DB의 행 해시 집계는 변경 탐지용이며 원본 파일의 SHA-256과 같은 검증 수단으로 혼동하지 않는다. 모든 조건을 만족할 때만 completed를 true로 기록하며 운영 승격은 별도다.

## 시간과 용량의 측정 범위

입력 확인과 복사, dbt build와 docs, 표별 내보내기와 전수 비교, 마지막 무결성 확인을 구분한다. dbt run_results에는 각 모델과 검사 노드의 실행 시간도 남긴다. 초기 실패 후 복사한 입력을 재사용한 경우에는 그 실행 시간과 최초 준비 시간을 따로 기록한다. 기존 귀속 갱신의 649.68초는 일부 마트만 재계산한 시간이므로 이번 전체 생성 및 145개 검사와 속도 비율로 비교하지 않는다.

최종 Parquet 14개의 바이트 합은 기존 Parquet 14개와 같은 범위로 비교한다. 새 DB 전체는 pinned 입력과 중간 결과 및 최종 표가 함께 들어 있으므로 별도 수치다. 출력 경로 전체와 임시 디스크 사용량은 5초 간격으로 관측한 최고값을 기록한다. 이는 관측 사이의 순간 최고값을 보장하는 수치가 아니다.

## 재현

Git Bash에서 기존 분리 환경으로 새 출력 경로를 사용한다. 실패 결과는 completed=false로 보존한다.

```bash
PYTHONPATH=src data/dbt-runtime/Scripts/python scripts/build_dbt_full.py \
  --baseline data/governance-baseline-20261022-r2 \
  --output data/dbt-full-new --memory-limit 16GB

venv/Scripts/python -m pytest tests/test_dbt_full.py tests/test_dbt_pilot.py \
  --basetemp data/pytest-dbt-full-new -q

data/dbt-runtime/Scripts/python -m pytest tests/test_dbt_full_runtime.py -m dbt \
  --basetemp data/pytest-dbt-full-runtime-new -q
```

종료와 실패 기록이 확인된 실험 DB만 --reuse-prepared로 새 경로에 복사해 재사용할 수 있다. 입력 매니페스트 지문과 원래 입력 및 복사 표의 내용을 확인하며 WAL이 남거나 실행 중인 결과는 거부한다. 이 옵션은 원래 운영 경로에 대한 재개나 덮어쓰기가 아니다.


전체 build에서 오류로 건너뛴 노드가 있는 닫힌 사본은 상태를 보존해 복구할 수 있다.

```bash
PYTHONPATH=src data/dbt-runtime/Scripts/python scripts/build_dbt_full.py \
  --baseline data/governance-baseline-20261022-r2 \
  --output data/dbt-full-recovery-new \
  --reuse-prepared data/dbt-full-failed \
  --resume-failed --memory-limit 16GB
```

성공한 전체 마트와 실제 시간 및 용량은 [전체 실측](measurements/2026-10-23-dbt-full.md)에 기록한다. 준비 및 8GB 실패 시도와 16GB 복구 시도를 구분하며 복구 시간만 최초 전체 생성 시간으로 쓰지 않는다.
