# GX 품질 검사와 결과 전환 경계

## 완료 범위

P4의 첫 실행은 P0 고정 표본과 같은 입력의 dbt 결과에 한정한다. Python 3.11.9의 별도 환경에서 GX Core 1.24.1, Pandas 3.1.0과 DuckDB 1.5.5를 검증했고 의존성은 `requirements-gx.txt`에 고정했다. Spark 환경과 dbt 환경은 유지했다. [실측](measurements/2026-10-23-gx-quality.md)을 따른다.

실제 입력은 월별 6,469행과 113,368회이며 월별 및 연간 14개 dbt 마트를 Python 독립 기대값과 대조한다. 월별 입력은 최대 10,000행, 개별 표본 Parquet은 100,000행 및 전체 합 1,000,000행으로 제한한다. 큰 표를 Python에 수집하기 전에 행 수를 확인하며 전체 Silver 80,563,985행을 Pandas로 가져오는 실행은 지원하지 않는다. 실제 Spark DataFrame이나 대규모 SQL 실행 엔진의 GX 호환성 및 운영 전체 연결은 이번 검증 범위에 포함하지 않는다.

## 규칙별 실행 책임

P1의 17개 규칙과 판정은 유지하고 `quality_gate.ownership()`에서 담당을 표시한다. GX는 DQ003 필수 키 및 월, DQ004 복합 키 유일성과 DQ005 양의 정수 횟수를 실제 표본에 검사한다. GX Expectation은 총 12개다. 원본 Python 타입을 확인하는 보조 열을 두어 bool이나 소수가 Pandas 숫자 변환으로 정상 정수처럼 보이는 경우를 막는다.

30초와 격리 사유는 기존 Spark 및 Python 정제가 담당하고 월별 표에 없는 ms_played를 새로 만들지 않는다. 모델 참조와 합계, 범위 및 버전은 dbt가 담당하며 P1의 독립 Python 검사를 작은 표본에서 유지한다. 입력 지문과 정책은 입력 가드, 사람 승인 범위는 검수 저널, 통계 보정 및 수집 연속성은 프로파일과 수집 담당으로 둔다. DQ008의 상세 미보유 SKIP은 명시적으로 허용하며 다른 필수 검사의 누락과 SKIP은 완료를 막는다. 경고 둘은 보고하되 임의의 실패 조건으로 올리지 않는다.

## 보고와 완료 및 결과 전환

GX Checkpoint는 세 Validation Definition을 실행하고 UpdateDataDocsAction으로 로컬 Data Docs를 생성한다. 원본 행이 불량 표본으로 HTML에 들어가지 않도록 BOOLEAN_ONLY 결과 형식을 사용하고 analytics는 끈다. GX 기본 보고와 실행 문맥 및 P1 결과, 규칙별 담당, 검증한 입력 사본은 출력 경로에 저장한다. 계정 원문이 들어 있는 입력 사본은 로컬에 유지한다. [GX 검증과 액션](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions/)

완료는 P1 필수 규칙과 GX의 세 규칙이 빠짐없이 통과하고 입력 및 후보 파일 지문이 유지된 경우에만 참이다. 검사기 예외나 불완전 결과도 실패다. `publishable`은 이 작은 사본의 검사 통과를 뜻하며 `promotion_ready` 및 `production_applied`는 계속 거짓이다. 운영 승격 허가를 뜻하지 않는다.

`quality_gate.promote()`는 호출자가 지정한 sandbox_root 안의 후보와 조회 대상 파일에만 사용한다. 후보의 모든 파일 SHA-256을 다시 확인하고 검사 결과로 완료 여부를 다시 판정한 뒤 임시 파일을 원자적으로 교체한다. 실패하거나 파일이 변조되면 이전 조회 대상의 바이트를 보존한다. 현재 명령에는 운영 조회 대상 교체 옵션을 제공하지 않는다. 여러 동시 승격 프로세스의 경쟁 제어와 실제 운영 스냅샷 잠금은 후속이다.

## 실행

Git Bash에서 다음처럼 실행한다. 출력은 존재하지 않는 새 경로를 사용한다.

```bash
data/dbt-runtime/Scripts/python -m venv data/gx-runtime
data/gx-runtime/Scripts/python -m pip install -r requirements-gx.txt
PYTHONPATH=src GX_ANALYTICS_ENABLED=false data/gx-runtime/Scripts/python scripts/check_gx_quality.py \
  --baseline data/governance-baseline-20261022-r2 \
  --candidate-db data/dbt-yearly-20261023/pilot.duckdb \
  --output data/gx-quality-new
PYTHONPATH=src GX_ANALYTICS_ENABLED=false data/gx-runtime/Scripts/python scripts/drills/gx_quality.py \
  --validated data/gx-quality-new --output data/gx-drill-new
```

dbt 후보를 지정하면 후보의 완료 기록 및 inputs.json 지문과 그 안의 P0 기준 지문을 각각 확인한다. DB는 읽기 전용으로 열고 전후 SHA-256을 비교한다. 후보를 생략하면 동결한 기대값의 품질 검사이며 실제 dbt 출력 검증과 구분한다.

빠른 로직 및 입력 가드는 기본 pytest로 검사하고 실제 GX 실행은 분리한다.

```bash
venv/Scripts/python -m pytest tests/test_quality_gate.py tests/test_gx_pilot.py tests/test_quality_contract.py
GX_ANALYTICS_ENABLED=false data/gx-runtime/Scripts/python -m pytest tests/test_gx_runtime.py -m gx
```

현재 P4는 고정 표본의 실행 및 실패 차단까지 완료했고 다음은 P5 영향 기간 Gold 증분이다. 운영 관측과 CI 및 복원 기준은 함께 준비한다.
