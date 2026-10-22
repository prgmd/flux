# 2026-10-22 고정 표본 품질 계약 검증

## 입력과 결과

[P0 고정 자료](2026-10-22-governance-baseline.md)의 성공 출력인 data/governance-baseline-20261022-r2를 사용했다. 월별 입력 6,469행, 총 청취 113,368회와 14개 기대 마트다. [품질 계약](../quality_contract.md)의 17개 규칙을 실행했다. 원본 Delta, 운영 Silver 및 Gold는 열거나 수정하지 않았다.

| 판정 | 규칙 수 | 내용 |
| --- | --- | --- |
| PASS | 14 | 필수 키, 중복, 횟수, 참조, 격리 사유, 보존 합계, 범위와 14개 독립 기대값, 버전 라벨 및 승인 범위 등 |
| FAIL | 0 | 발견 없음 |
| WARN | 2 | DQ016 보관 구간 누락 2개와 다음 연속 입력 미확정, DQ017 통계 경고 기준 미보정 |
| SKIP | 1 | DQ008 월별 표에 개별 재생 시간이 없어 정상 상세 품질을 판정하지 않음 |

메타데이터 변형 498행 중 recording MBID가 없는 155행은 허용했다. 원문 표본 256행 중 재생 시간 없는 128행도 허용했으며 해석 불가능한 재생 시간은 없었다. 이 원문 표본을 전부 정상 상세로 취급하지 않았다. 격리 표본 64행의 재판정과 저장 사유 불일치는 0이다. 표본 비율은 전체 코퍼스의 비율로 해석하지 않는다.

품질 통과와 고정 입력 불변을 확인했다. 이 결과는 표본 검증이며 promotion_ready=false다. 데이터 수급 연속성, 도구 호환성 및 운영 승격 준비가 끝났다는 뜻이 아니다.

## 실패와 경고 주입

실제 고정 입력을 읽은 뒤 각 시나리오마다 메모리 사본을 만들어 오류를 주입했다. 빈 사용자, 월별 키 중복, 0과 소수 횟수, 누락된 사전 참조, 잘못된 합계와 비중, 잘못된 순위, 연간 계정 중복 가산, 바뀐 버전 라벨, 미승인 매핑, 격리 사유 변조, 정상 상세에 29,999ms 포함, 잘못된 계정 키의 14개 실패 시나리오를 검사했다. 비숫자 재생 시간은 경고, 재생 시간 없음 및 문자열 30,000과 30,001은 통과하는 대조 시나리오도 확인했다. **총 16개 모두 기대 판정과 일치**했고 입력 파일 지문은 유지됐다.

단위 테스트에서는 검사기가 실패를 놓쳐도 실험이 통과했다고 보고하지 않는지 확인했다. 검사 전 입력 부재, 빈 상세 입력의 SKIP, bool 승인 수 거부, 검사 후 지문 변조와 요약 일치도 검사했다. 추가 검토에서 NULL 곡 키끼리 연결되면 참조가 정상으로 오인되는 문제와 Parquet 읽기 오류가 실패 보고서 없이 끝나는 문제를 먼저 실패 테스트로 재현하고 수정했다.

## 재현과 한계

최종 성공 결과는 data/quality-contract-20261022-r3, 실패 주입은 data/quality-contract-drill-20261022-r2다. 모든 검사 결과는 입력과 분리된 로컬 산출물에 남기고 데이터는 Git에 올리지 않는다. 재실행은 새 출력 폴더명을 쓴다.

```bash
PYTHONPATH=src venv/Scripts/python -m pytest \
  tests/test_quality_contract.py tests/test_baseline.py tests/test_monthly_gold.py \
  tests/test_common_deltalog.py tests/test_review_journal.py tests/test_review_evidence.py \
  tests/test_review_sandbox.py tests/test_review_pilot.py tests/test_review_cli.py \
  tests/test_recording_link_policy.py tests/test_artist_attribution.py tests/test_manifest.py \
  tests/test_catalog_window.py tests/test_silver_quality.py -q
```

관련 테스트 **180개 통과**이며 새 품질 계약 테스트는 22개다. 전체 프로젝트 테스트 실행 수가 아니다. Spark 판정 코드 수정, 실제 CDF 입력 실험, dbt 및 GX 호환성, 전체 코퍼스 재검사와 사람 승인 매핑의 적용은 이번 검증에 포함하지 않았다.
