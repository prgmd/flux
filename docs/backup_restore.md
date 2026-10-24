# 작은 Delta 표본의 독립 백업과 복원

2026-10-24에 기존의 소형 CDF 검증 사본을 백업하고 새 경로에 복원했다. 파일 지문 대조와 별도 Delta 리더의 실제 읽기에서 모든 키와 `play_count`, 나머지 컬럼도 일치했다. 운영 Bronze, Silver와 Gold의 경로, 파일 및 조회 대상은 바꾸지 않았다.

## 백업 단위

현재값만 복사하지 않고 작은 표본에 남아 있는 이력 전체를 보관한다. [Delta 프로토콜](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)의 데이터 파일, 로그와 변경 파일 구조를 따르며 일반적인 모든 Delta 백업 방식을 대체하는 도구는 아니다.

| 포함 대상 | 복원에서 확인하는 내용 |
| --- | --- |
| Bronze와 Silver 세 표 | 현재 활성 파일과 보관 중인 과거 파일, 빈 격리 표 |
| `_delta_log` 및 CDF 파일 | 0부터 종료 버전까지의 JSON 이력, 로그가 참조하는 데이터와 CDF 파일 |
| `context/cursor.json` | 원본 Bronze ID와 처리 버전, Silver 표 ID와 버전 및 로그 지문 |
| 규칙 파일 12개 | 30초 품질 규칙, 중복 및 집계, 곡 연결과 검수 규칙의 실제 파일 |
| `context/approved_mappings.json` | 기존 운영용 승인 내보내기 전체, 제외 사유 포함 |
| `context/expected.json` | 백업 전 네 표의 고정된 모든 컬럼과 행, 월별 키와 횟수의 대조 기준 |
| 완료 결과와 매니페스트 | 전체 파일 목록과 바이트 수 및 SHA-256, 마지막 완료 여부 |

현재 운영용 승인 후보와 매핑은 0건이다. 빈 승인 상태를 그대로 복사하며 새 승인을 만들지 않는다. 승인 파일의 형식과 지문 보존을 확인하지만 사람의 판단과 근거를 다시 심사하는 절차는 아니다. 처리 위치는 Bronze에서 월별 Silver까지 반영한 버전이다. Gold 소비 완료 위치나 Spark 엔진의 Parquet checkpoint 파일과 혼동하지 않는다.

복원 위치는 `bronze`와 `silver/<표 이름>`의 상대 경로로 연결한다. Delta ID와 버전 및 로그는 바꾸지 않는다. 파일을 새 표로 다시 적재해서 ID를 새로 발급하는 방식은 사용하지 않는다. 규칙 파일도 실행하지 않고 원래 바이트와 지문을 보존한다.

## 완료와 실패 조건

표별 최대 10,000행과 합계 100MiB 이하인 작은 사본에 한정한다. 0부터 종료까지의 JSON 로그와 그 이력의 add 및 cdc가 참조하는 파일이 모두 있어야 한다. 삭제 벡터, 누락된 JSON 이력이나 이미 VACUUM된 과거 파일이 있는 표는 지원 범위 밖이다. checkpoint 파일만 남은 일반 대형 표를 다루는 백업 도구는 아니다.

Silver 세 표의 처리 상태가 Bronze ID 및 버전, UTC와 30초 정책에 일치해야 한다. 쓰기 잠금이 있으면 거부하고 복사 전후 파일 목록과 지문을 비교한다. 백업 동안 입력은 정지된 사본을 사용한다. 여러 표를 동시에 커밋하는 분산 트랜잭션을 구현한 것은 아니다.

백업과 복원 출력은 기존 경로에 덮어쓰지 않는다. 상대 경로 밖의 참조와 심볼릭 링크를 거부한다. 파일 누락과 변경, 추가 파일, 미완료 백업도 복원 전에 차단한다. 복사 중 원본이 바뀌면 완료를 남기지 않는다. 전체 매니페스트 검증과 키 및 값 대조를 통과한 뒤에만 완료한다. 지문은 우발적 변경 검출 수단이며 외부 서명이나 출처 인증은 아니다.

## 실제 표본 결과

원본은 보존 중인 `data/gold-cdf-source-20261023-r1`이고 최종 백업 및 복원과 검증 결과는 `data/restore-drill-20261024-r2`에 있다. 보존 실청취 표본과 앞선 통제 변경이 포함돼 있으며 새 덤프나 전체 운영 데이터가 아니다.

| 표 | 종료 버전 | 현재 행 | 0번 버전 행 | 읽은 이력 버전 수 |
| --- | ---: | ---: | ---: | ---: |
| Bronze | 16 | 258 | 256 | 17 |
| 월별 Silver | 20 | 256 | 208 | 21 |
| 곡 메타데이터 | 16 | 464 | 208 | 17 |
| 격리 | 16 | 0 | 1 | 17 |

월별 키 256개와 각 `play_count`가 전부 일치하고 합계는 258회다. 다른 표도 모든 컬럼과 행이 일치한다. 복원된 파일만 사용하는 delta-rs 1.6.6에서 현재 및 0번 버전 읽기와 보관된 CDF 2,204행의 읽기를 확인했다. 이 CDF 행에는 과거 교체와 수정 전후 이미지가 들어 있어 새 청취 횟수로 합산하지 않는다.

백업 내용 파일은 521개와 1,826,215바이트다. 매니페스트와 결과를 포함한 백업 폴더 전체는 1,940,678바이트, 복원 폴더는 1,940,570바이트다. 압축은 하지 않았다. 두 폴더의 결과 보고서는 역할이 달라 전체 바이트 수가 다르지만 실제 내용 파일들의 지문은 같다.

실행 기록의 함수 전체 경과는 백업 18.033초, 복원 21.603초, 독립 Delta 읽기 검증 17.351초다. 내부 보고서의 14.406초, 14.578초와 10.141초는 사전 검사 뒤부터 잰 하위 구간이므로 전체 시간과 바꿔 쓰지 않는다. 패키지 설치나 Docker 대기 시간은 이 숫자에 포함하지 않는다.

## 실패에서 확인한 경계

별도 리더로 읽었을 때 처음에는 Bronze 타임스탬프 비교가 실패했다. 파일 직접 읽기는 시간대 없는 Python datetime으로, Delta 리더는 UTC가 붙은 값으로 반환했다. Delta 스키마의 `timestamp`만 UTC 표현으로 통일하고 `timestamp_ntz`는 시간대 없는 시각으로 유지했다. 원본 파일은 고치지 않았다. 큰 정수도 문자열이나 부동소수로 바꾸지 않는 회귀를 추가했다.

Docker 엔진은 Inference 소켓 초기화 오류로 준비되지 않았다. 초기화나 설정 변경 대신 별도 Python 3.11 환경에서 [Delta Python 바인딩](https://pypi.org/project/deltalake/1.6.6/)을 사용했다. Windows에서 Arrow의 UTC 처리에 필요한 tzdata도 해당 환경에만 설치했다. Spark에서의 이번 복원 재실행은 하지 않았으며 별도 실제 Delta 리더의 결과로 구분한다.

`tests/test_snapshot_backup.py`의 기본 검사 20개가 37.04초에 통과했다. 데이터와 로그, 과거 데이터와 CDF 파일, 규칙과 승인 및 처리 위치 누락, 추가 파일과 미완료 상태, 쓰기 중 원본 변경, 같은 합계의 다른 키와 시간대 표현을 검사한다. `tests/test_delta_restore_runtime.py`는 실제 Delta 생성 뒤 원본 폴더를 다른 이름으로 옮겨 사용할 수 없게 하고 복원 및 이력과 CDF, 빈 표를 검증했으며 1개가 2.31초에 통과했다. 실제 표본 검증에서도 리더가 선택한 모든 활성 경로가 복원된 표 아래에 있는지 확인한다.

로컬 기본 전체 회귀는 1,060 passed, 22 skipped, 12 deselected in 134.58s였다. 별도 Delta 환경의 검사는 이 기본 통과 수에 합치지 않는다. 기존 미추적 조사 초안과 실제 표본 유무 때문에 깨끗한 CI의 수와 구분한다.

[원격 CI 실행](https://github.com/prgmd/flux/actions/runs/40076870803)은 기본 1,056 passed와 23 skipped 및 12 deselected가 24.46초에, dbt 7개가 168.80초에, GX 11개가 11.69초에 통과했다. Delta 복원 엔진 검사는 전용 의존성을 설치한 로컬 환경에서 명시 실행했으며 기본 CI에서 skip된 것을 통과로 합치지 않았다.

## 재현

Python 환경에 기본 개발 의존성을 설치하고 실제 Delta 읽기용 환경에는 `requirements-restore.txt`를 추가한다. 아래는 Git Bash에서 별도 표본 경로만 사용하는 예시다.

```bash
PYTHONPATH=src python scripts/drills/backup_restore.py backup \
  --source data/gold-cdf-source-20261023-r1 \
  --approval data/governance-baseline-20261022-r2/source_manifests/r9_production_export.json \
  --output data/restore-example/backup
PYTHONPATH=src python scripts/drills/backup_restore.py restore \
  --source data/restore-example/backup --output data/restore-example/restored
PYTHONPATH=src python scripts/drills/backup_restore.py verify-delta \
  --source data/restore-example/restored --output data/restore-example/verification
python -m pytest tests/test_snapshot_backup.py -q
python -m pytest tests/test_delta_restore_runtime.py -m delta_restore -q
```

세 실행은 [실행 기록](execution_records.md)에 입력과 단계 및 오류를 남긴다. 원본 덤프나 전체 Gold, dbt 환경과 서비스의 복구는 포함하지 않는다. 같은 로컬 디스크의 새 경로 검증이므로 별도 매체 및 호스트 장애 대비나 백업 보존 기간, 허용할 데이터 손실 구간과 목표 복구 시간을 확정한 결과도 아니다. 실제 대형 백업의 매체와 보존 및 복구 시간은 별도 결정한다. P6a의 작은 독립 복원 확인 뒤 다음 기능 단위는 P6b의 dbt 문서와 데이터 카탈로그 연결이며 운영 추적의 남은 범위도 유지한다.
