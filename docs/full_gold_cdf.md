# 전체 Silver의 CDF와 Gold 대조

`scripts/drills/full_gold_cdf.py`는 완료된 전체 Gold 검증 DB와 월별 Silver의 별도 사본을 사용하는 명시 실행 경로다. 기존 소형 Python 검증기의 행 수 제한을 유지하며 전체 행을 Python 목록으로 읽지 않는다. 실제 Delta CDF는 Spark가 Parquet로 내보내고 SQL 엔진에서 시작 및 종료 스냅샷과 대조한다. 운영 경로의 자동 교체 기능은 없다.

## 입력과 변경 범위

`prepare`는 고정 기준과 기존 14개 전체 대조의 완료, DB 및 입력과 프로젝트의 지문을 확인한다. 월별 Silver 세 표를 새 경로에 복사하고 원래 파일과 지문을 대조한다. 기본 제어 변경은 기존 공식 크레딧에 연결된 세 행의 횟수 수정, 삭제 및 다른 연도 이동과 기존 곡을 듣는 새 제어 계정의 삽입이다. 수집한 새 덤프의 변경이라고 해석하지 않는다. 곡 사전과 라벨 및 공식 귀속은 고정하며 새 MSID와 승인 정책의 확장은 별도 과제다.

Spark는 사본에 실제 `MERGE`를 적용하고 `versionAsOf`로 정한 시작 및 종료 버전 사이의 CDF를 읽는다. 원래 Bronze 처리 위치는 변경하지 않는다. 이 실험은 Silver 단계의 통제 변경이며 Bronze부터 새 덤프를 다시 처리하는 실험과 구분한다. 전체 행을 수집하는 대신 두 시점의 합계와 CDF 유형별 개수만 가져온다.

## 적용과 대조

Gold 입력이 시작 버전의 Silver와 모든 키 및 값에서 같아야 한다. CDF를 커밋 순서로 읽고 수정 전 값과 삭제 대상이 현재 입력과 같은지 확인한 뒤 제거한다. 같은 커밋의 새 합계를 삽입하며 이미 존재하는 키는 거부한다. 전체 변경 범위를 하나의 DuckDB 트랜잭션으로 적용하므로 뒤 커밋의 오류도 앞선 변경까지 되돌린다. 종료 위치의 같은 범위 재실행은 0행이며 중간 커서는 거부한다.

CDF 적용 결과는 종료 버전의 전체 활성 Delta 파일과 전수 대조한다. 다른 DB에는 이 종료 스냅샷을 직접 적재해 비교 기준을 만든다. 두 DB는 동일한 dbt 프로젝트와 고정 사전으로 실행한다. 증분 DB는 영향 월과 연도의 최종 기간을 교체하며 비교 DB는 `--full-refresh`로 전체 모델을 다시 만든다. 연간 고유 계정 수는 사용자별 연간 기여로 재계산한다.

두 경로의 모델 24개와 검사 145개를 각각 확인하고 최종 14개 마트의 모든 기간, 키 및 컬럼을 대조한다. 변경하지 않은 기간도 이전 전체 Gold와 대조한다. 정수와 문자열은 정확히 비교하며 부동소수는 기존 전체 대조의 절대 및 상대 허용 오차 `1e-12`를 사용한다. 파일의 물리적 정렬과 압축은 논리적 일치와 별도로 기록한다. 모든 검사를 마치기 전에는 완료 처리 위치를 발행하지 않는다.

## 시간과 저장량 해석

복사, 시작 입력 검사, CDF 적용, 종료 입력 검사, 증분 build와 검사, 전체 rebuild와 검사, 파일 내보내기 및 전수 대조 시간을 나눈다. 증분 경로에서도 현행 중간 모델 6개와 데이터 검사는 전체 입력을 처리한다. 따라서 최종 기간 교체를 전체 실행의 모든 연산이 변경 행에만 비례하는 최적화로 해석하지 않는다. 고정된 작은 변경량으로 실행한 결과는 대량의 일일 CDF 처리율을 뜻하지 않는다.

최종 Gold Parquet와 입력 및 중간 결과를 포함한 DuckDB, Silver 활성 파일과 이력 포함 사본 및 CDF를 구분해 바이트로 기록한다. 작업 경로와 spill 사용량의 최대치는 5초 간격 관측값이며 RAM 최대치가 아니다. 중단된 시도의 경로도 보존하고 성공 실행의 시간과 합치지 않는다.

## 실행

dbt 환경에서 기준을 준비한다.

```bash
PYTHONPATH=src python scripts/drills/full_gold_cdf.py prepare \
  --base data/completed-full-dbt --baseline data/fixed-baseline \
  --output data/new-full-cdf-source
```

Spark 환경에는 이 사본만 쓰기 가능하게 연결하고 `scripts/drills/full_silver_cdf_probe.py --silver <사본/silver> --changes <사본/changes.json> --output <사본/probe>`를 실행한다. 이어 dbt 환경에서 실행한다.

```bash
PYTHONPATH=src python scripts/drills/full_gold_cdf.py consume \
  --source data/new-full-cdf-source --output data/new-full-cdf-gold
```

기본 검사는 `tests/test_full_gold_cdf.py`, 실제 작은 Delta 검사는 `tests/test_spark_full_gold_cdf.py -m spark`다. 전체 실행은 수십 GB의 로컬 파일과 두 엔진이 필요한 별도 실험이며 CI에서 자동 실행하지 않는다. 소형 경로의 강제 종료 및 복구 검증은 [기존 실제 CDF 검증](silver_cdf_gold.md)에서 유지한다.

## 전체 계산 중단 사본의 제한된 복구

전체 재계산 도중 프로세스가 종료됐고 기존 프로세스가 없음을 확인한 경우 `resume-tests`를 사용할 수 있다. 완료된 증분 실행과 고정 CDF 및 입력, 실행 프로젝트의 지문이 유지돼야 한다. 시작 단계가 전체 dbt 실행이어야 하며 완료 처리 위치가 존재하거나 이미 복구를 시도한 사본은 거부한다. 이전 미완료 결과와 로그는 보존한다.

```bash
PYTHONPATH=src python scripts/drills/full_gold_cdf.py resume-tests \
  --source data/new-full-cdf-source --output data/new-full-cdf-gold
```

로그에서 실제 성공한 모델 이름을 manifest와 대조한다. 프로젝트의 전체 모델 개수를 완료 개수로 해석하지 않는다. dbt build의 모델과 검사는 의존 관계에 따라 섞여 실행되므로 중단 시점에 미완료 모델이 남을 수 있다. 복구 경로는 완료된 모델을 재사용하고 미완료 모델을 전체 계산 방식으로 생성하며 검사 145개를 모두 다시 실행한다. 이전 검사 일부를 성공으로 재사용하지 않는다. 이어 두 DB의 입력 지문과 최종 14개 마트 및 영향 밖 기간을 확인하고 마지막에만 완료 위치를 저장한다.

재사용 전에 전체 계산 DB의 입력을 고정 종료 Silver와 전수 대조한다. 실행된 SQL과 현재 사본의 SQL도 확인한다. dbt가 정리하는 바깥 공백과 Windows 줄바꿈 차이는 허용하며 SQL 내부의 로직 변경은 거부한다. `tests/test_full_cdf_recovery.py`는 완료 모델 추출, 오류 및 중복 로그, SQL 변경과 완료 상태 및 중복 복구 차단을 검사한다.

복구 뒤 `elapsed_seconds`와 저장량 최대치는 복구 세션만 측정한다. 중단 전에 완료된 구간의 시간은 `seconds`에 유지하지만 전체 재계산의 연속 실행 시간은 복원하지 않는다. 이전 프로세스의 최대 임시 저장량도 알 수 없다. 이 수동 복구는 운영 자동 재시도나 모든 중단 지점을 포괄하는 복구 기능을 뜻하지 않는다.

Windows에서는 실행 중 화면과 시스템의 활성 상태를 함께 요청하고 종료 시 해제한다. 전원 계획은 바꾸지 않는다. 요청 성공을 실제 비수면의 증거로 사용하지 않으며 전원 이벤트와 단계별 진행도 확인한다. 대기 구간이 있으면 총경과와 별도로 기록하고 순수 연산 시간으로 해석하지 않는다. 사용자 의도의 덮개 닫기나 전원 버튼 조작을 막는 기능은 아니다. [Windows 실행 상태 요청](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-setthreadexecutionstate)을 따른다.
