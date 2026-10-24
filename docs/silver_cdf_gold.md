# 실제 Silver CDF와 Gold의 연결

이 실행기는 완료된 월별 Silver의 실제 Delta 변경 기록을 읽어 dbt Gold를 갱신하는 소형 검증 경로다. Spark가 변경분을 읽고 dbt의 별도 Python 환경이 소비한다. 운영 Bronze, Silver와 Gold의 경로나 조회 대상은 변경하지 않는다. 월별 최대 10,000행과 CDF 최대 100,000행으로 제한하며 전체 80,563,985행의 성능 또는 운영 완료를 뜻하지 않는다.

전체 80,563,985행은 별도의 [SQL 및 Delta 경로](full_gold_cdf.md)에서 통제 변경 6행을 실제 CDF로 전달하고 14개 전체 계산과 전수 대조했다. 이 문서의 소형 경로 제한과 강제 종료 검증 범위는 그대로 유지한다. [전체 실측](measurements/2026-10-23-full-silver-cdf-gold.md)을 함께 읽는다.

## 버전이 다른 두 엔진을 연결하는 방법

Silver의 월별 횟수, 메타데이터 및 격리 세 표가 같은 Bronze 처리 위치에 도달했는지 먼저 확인한다. 표별 Delta ID와 버전, 해당 버전까지의 로그 지문 및 품질 정책을 고정한다. Spark는 `versionAsOf`로 시작과 종료 시점의 월별 값을 읽고 `readChangeFeed`에 시작 다음 버전과 종료 버전을 명시한다. 실제 읽기가 실패하면 완료한 변경 묶음을 만들지 않는다. 변경 행이 없는 메타데이터 커밋과 같은 버전 재실행도 구분한다.

내보내는 자료에는 변경 전과 후의 월별 값, CDF 및 종료 버전의 메타데이터와 파일 지문이 있다. 두 전체 값은 소형 검증용 독립 대조에 사용한다. 파일 지문만으로 출처를 증명하는 인증 체계는 아니며 이 경로의 실제 Spark 실행과 함께 해석한다. 시작 버전의 Gold 입력이 Silver와 같아야 하고 CDF의 적용 결과가 종료 버전의 Silver 전체 값과 같아야 한다. 누락 범위와 다른 표 ID, 변경된 로그 및 파일은 실패한다.

## 반복 가산을 막는 범위

CDF를 커밋 순서로 처리하고 각 커밋에서 삭제 및 수정 전 값을 검증한 뒤 새 값을 넣는다. 월별 `play_count`의 수정 후 값은 새로운 합계다. 이 값을 기존 합계에 다시 더하지 않는다. 사용자나 월이 바뀌면 이전 키를 제거하고 새 키를 만든다. 같은 CDF 묶음을 이미 소비한 종료 위치에서 다시 실행하면 현재 입력을 대조하고 변경을 다시 적용하지 않는다. 더 오래된 묶음이나 중간 범위를 건너뛴 묶음은 거부한다.

Gold는 완료한 이전 DB의 사본에서 영향 월과 해당 연도를 교체한다. 순위와 계정 수 및 집중도는 기간 전체에서 다시 계산한다. dbt의 모델 및 데이터 검사와 Python 전체 계산, 기존 SQL 전체 계산 및 영향 밖의 행 대조를 모두 통과해야 새 처리 위치를 완료한다. 연간 계정 수는 월별 최종 계정 수의 합이 아니다.

여러 모델의 성공과 CDF 처리 위치의 성공은 별개다. 바깥 `result.json`이 완료되지 않은 사본은 내부 Gold가 완성됐어도 다음 입력의 기준으로 사용할 수 없다. 실패나 강제 종료 후에는 마지막 완료 사본을 이전 결과로 지정하고 같은 CDF를 새 경로에 재실행한다. 운영 조회 대상 자동 교체 옵션은 없다.

## 곡 메타데이터와 귀속의 경계

기준 Gold의 검증된 곡 차원과 라벨 및 크레딧을 고정한다. 처음 등장한 MSID는 종료 버전의 Silver 메타데이터에서 비교 가능한 이름과 버전 표기를 보존하고 `msid:` 곡 키와 `unreviewed` 상태를 부여한다. 확인하지 않은 녹음 MBID나 아티스트 ID로 자동 연결하지 않는다. 청취는 곡과 커버리지에 남고 공식 귀속이 없으면 아티스트 마트에는 들어가지 않는다. Sped Up, Remix 및 Live를 원곡으로 합치지 않으며 표기 버전이 충돌하면 드러낸다. 기존 MSID의 공식 매핑과 대표 이름 보정은 별도의 버전 고정 검수 입력으로 이어갈 과제다.

## 실행과 검증

Spark 환경에서 `scripts/export_silver_cdf.py --silver <별도 Silver> --output <새 내보내기>`를 실행한다. 최초에는 처리 위치를 지정하지 않고 이후에는 마지막 완료 Gold 사본의 `cursor.json`을 `--cursor`로 준다. 원본과 출력 경로는 겹칠 수 없다.

dbt 환경에서는 다음과 같이 실행한다.

```bash
PYTHONPATH=src python scripts/update_gold_from_cdf.py \
  --export data/new-cdf-export --reference data/completed-reference-gold \
  --output data/new-cdf-gold

PYTHONPATH=src python scripts/update_gold_from_cdf.py \
  --export data/next-cdf-export --previous data/new-cdf-gold \
  --output data/next-cdf-gold
```

기본 테스트는 `tests/test_gold_cdf.py`, 실제 Spark 읽기는 `tests/test_spark_gold_cdf.py -m spark`로 나눈다. 실제 청취 표본의 수정과 삭제 묶음은 `scripts/drills/gold_cdf_source.py`가 만들고 `tests/test_gold_cdf_runtime.py -m dbt`에서 소비 및 종료 복구를 검증한다. 실제 Spark 내보내기 경로인 `FLUX_CDF_EXPORT_ROOT`와 완료 기준 Gold인 `FLUX_CDF_REFERENCE_GOLD`가 없으면 이 통합 검사는 skip하며 성공으로 세지 않는다. 대규모 데이터를 CI에 올리지 않는다.

[실제 검증 결과와 한계](measurements/2026-10-23-silver-cdf-gold.md), [Gold 기간 교체 규칙](gold_increment.md), [확장 계획](data_governance_plan.md)과 함께 읽는다.
