# 표본 검수의 근거와 판단 이력

이름 비교로 찾은 후보의 근거와 검수자의 판단을 분리해 저장한다. [R9](recording_link_rules.md#r9-공식-id를-확인한-악센트-후보-연결)의 eligible은 조건 충족이며 사람의 승인과 같지 않다. 실제 사람 승인이 없는 기록에 human을 표시하지 않는다. 모든 원문과 원래 관측 MBID를 보존한다.

## 검수 절차

후보의 원문과 공식 아티스트 및 녹음 근거, 버전을 대조한다. R9의 상태와 사유를 표시한 뒤 검수자가 승인, 분리 유지, 보류 중 하나를 이유와 함께 기록한다. 승인 기록을 바꿀 때는 이전 판단 ID를 명시해 새 기록을 추가한다. 기존 행을 덮어쓰지 않는다. 보류 이유에는 다음에 확인할 내용을 적는다.

| 검수 상태 | 저장 값 | 조건 |
| --- | --- | --- |
| 승인 | approved | R9 eligible인 후보만 가능 |
| 분리 유지 | keep_separate | R9에서 확인된 ID 또는 버전 차이가 있어야 함 |
| 보류 | hold | 누락, 미확인, 추가 검수 필요. eligible이어도 검수자가 보류할 수 있음 |

기록마다 reviewer_kind를 human 또는 agent로 저장한다. 프로그램이 추출한 eligible을 곧바로 사람 승인이라고 쓰지 않는다. production 내보내기는 최신의 유효한 사람 승인만 사용한다. sandbox 내보내기는 agent 승인도 허용하며 항상 실험용이라고 표시한다. reviewer_kind는 신원 인증 기능이 아니며 실제 운영자가 올바른 검수 주체를 기록해야 한다.

## 저장 형식

초기 구현은 로컬 JSONL이다. UTF-8 한 줄에 JSON 객체 하나를 기록한다. 원본과 중간 산출물은 로컬 data 아래 보관하고 저장소에는 로직과 문서만 둔다.

| 파일 | 주요 필드 |
| --- | --- |
| candidates.jsonl | schema_version, candidate_id, evidence_id, observation_ids, observations, facts, source_state, proofs, assessment, rule_version |
| decisions.jsonl | decision_id, candidate_id, evidence_id, status, reason_code, note, reviewer_kind, reviewer_id, reviewed_at, supersedes, rule_version |

관측 ID는 MSID와 원래 이름, 관측 MBID, 원본 artist_mbids와 mbid_source 조합에서 만든다. 후보 ID는 관측 ID 두 개를 정렬해 만든다. 근거 ID에는 공식 근거와 조회 기준 및 규칙도 포함한다. 비교 문자열을 원본 식별자로 쓰지 않는다. 같은 근거 재등록은 새 행을 만들지 않으며, 공식 자료나 근거가 바뀌면 같은 후보에 새 근거 ID가 생긴다.

source_state에는 Silver 로그 지문, 공식 스냅샷 식별자와 입력 표본 해시를 둔다. facts는 R9 입력이고 proofs는 공식 녹음, 크레딧, redirect와 버전 검수 근거다. 판정 함수에 넘긴 원래 MBID와 이름이 observations의 원문과 다르면 등록을 거절한다. 해시는 우발적 변경을 찾는 장치이며 검수자의 신원이나 외부 자료의 진실성을 인증하는 서명은 아니다.

검수 시각은 시간대가 명시된 UTC로 저장한다. 최신 상태는 파일에 추가된 순서와 supersedes 연결로 판단한다. 시각 문자열만 정렬해 이력을 바꾸지 않는다. 판정을 수정하려면 직전 판단 ID가 맞아야 하므로 다른 검수의 최신 결정을 모른 채 덮어쓰지 않는다.

## 재검수와 파일 보존

새 근거가 등록되면 이전 근거의 승인을 자동 재사용하지 않는다. source_state나 규칙 버전이 달라져도 승인 내보내기에서 제외한다. 이전 승인 뒤 보류가 추가되면 과거 승인을 찾아 되살리지 않는다. 한 관측에 서로 다른 승인 대상이 있으면 내보내기를 중단한다. 내보내기는 관측 단위이며 MSID 전체의 월별 집계를 자동 변경하지 않는다.

쓰기 중에는 저널 디렉터리의 잠금을 사용한다. 이미 잠긴 경우 대기하거나 강제로 해제하지 않고 오류를 반환한다. 각 기록은 추가한 뒤 flush와 fsync로 저장한다. 근거 저장 직후 중단돼 판단이 없다면 미검수로 남는다. 마지막 줄이 덜 써졌거나 JSON, 해시 및 판단 연결이 손상됐으면 다음 쓰기와 내보내기를 중단한다. 손상된 끝을 조용히 지우거나 이전 판단을 추측해 복구하지 않는다. 잠금과 손상 복구는 보존된 사본 및 실제 실행 상태를 확인한 후 별도로 수행한다.

## 검증 범위

`tests/test_review_journal.py`는 모듈 부재 실패부터 확인했다. 원문 보존과 방향에 무관한 후보 ID, 같은 근거의 재등록, 판단 변경 이력, 최신 판단 요구, 미확인 승인 차단, agent와 human 구분, 새 근거 및 조회 기준 변경, 규칙 변경, 불완전한 줄과 변경된 근거, 잠금, 존재하지 않는 대상과 상충 승인 내보내기 차단을 검사한다. 새 테스트 9개와 기존 관련 회귀 합계 69개가 통과했다.

102행의 실제 표본을 공식 자료에 다시 대조하고 후보 51쌍의 agent 판단 및 별도 월별 사본 검증까지 완료했다. 전체 관련 회귀는 79개가 통과했다. [실제 결과와 한계](measurements/2026-10-22-recording-review.md)를 따른다. FastAPI와 사람 승인, 전수 적용 및 운영 데이터 변경은 포함하지 않는다.

## 공식 근거를 만드는 단계

`flux.review.evidence`는 원래 MBID가 현재 공식 녹음 ID와 같거나 공식 redirect의 숫자 대상이 해당 녹음 행과 같을 때만 녹음 근거를 확인한다. 원래 MBID가 NULL이면 현재 ID를 채워 넣지 않는다. 공식 아티스트 크레딧의 전체 ID 집합을 보존하며 로컬 이름을 공식 아티스트 이름 또는 이미 검수한 별칭과 대조한다. 악센트만 다른 이름은 제출된 아티스트 ID 집합도 공식 크레딧과 같아야 확인한다. 이름의 악센트 제거만으로 귀속을 확정하지 않는다.

공식 아티스트 대표 이름과 발매 크레딧의 표시 이름은 다를 수 있다. 기존 표본의 공식 이름은 artist 표의 대표 이름이므로 같은 열로 재대조한다. 실제 VOCES8와 Voces8의 차이에서 잘못된 변경 감지를 발견하고 테스트로 고정했다.

버전은 공식 제목과 설명, 로컬 버전 문구 및 video 여부를 함께 본다. Live, Remix와 Sped Up 등은 버전 키에 남긴다. 버전 표기가 없는 경우 `recording:<공식 MBID>`를 쓰며 녹음 ID에 한정된 키다. 이를 original 음원이라고 단정하지 않는다. 모르는 버전 설명, 영상 또는 원문과 공식 버전 표기의 모순은 보류한다.

## 월별 사본으로 옮기는 조건

관측 승인과 MSID 전체 승인은 다르다. 월별 표는 사용자+MSID+월까지 요약돼 있어 어느 메타데이터 관측에서 나온 횟수인지 복원할 수 없다. 따라서 승인 후보 MSID의 메타데이터 변형을 현재 활성 사전 전체에서 읽고, 모든 변형이 같은 공식 아티스트 집합과 녹음 및 버전으로 승인된 경우에만 MSID 전체를 옮긴다. NULL MBID나 미검수 변형 하나라도 있으면 제외한다. 같은 곡의 다른 완전한 MSID가 없으면 연결할 상대가 없으므로 제외한다.

`flux.review.sandbox`는 최대 100만 월별 행의 표본만 새 경로에 저장한다. before.parquet에서 독립 Python 집계를 만들고 DuckDB의 after.parquet와 모든 키를 대조한다. 같은 고정 입력을 재계산한 repeat.parquet도 대조한다. 청취 수 합은 그대로 보존한다. 이는 고정 표본의 곡 연결 및 재계산 검증이며 새 덤프의 CDF 증분이나 사건 중복 차감을 검증한 것이 아니다.

## 실행과 후속 사람 검수

Git Bash에서는 `PYTHONPATH=src venv/Scripts/python scripts/investigations/run_recording_review.py --help`로 입력 경로를 확인한다. sample은 102행 표본 디렉터리, prepared는 이전 실험의 공식 ID 조회 데이터베이스, silver는 고정 월별 Silver, snapshot과 redirects는 공식 추출 스냅샷, output은 존재하지 않는 별도 경로다. 이전 실험 전체의 완료 상태를 재사용하지 않으며 현재 활성 Silver 메타데이터를 다시 읽는다. 공식 파일의 해시와 선택한 행 및 크레딧을 재대조한다. 실행 전후 Silver 로그 지문, 표본과 매니페스트 해시, 검증한 공식 파일의 크기 및 수정 시각이 유지돼야 completed가 된다.

출력의 review.md에서 원문과 사유 및 근거 ID를 읽고 facts.json에서 공식 녹음과 크레딧, redirect를 확인한다. 사람이 직접 결정할 때는 `PYTHONPATH=src venv/Scripts/python -m flux.review.cli --help`의 필수 항목을 채운다. journal, evidence, status, reason, note, reviewer-kind와 reviewer-id를 모두 명시한다. 이미 판단이 있으면 decisions.jsonl의 최신 decision_id를 supersedes로 지정한다. 오래된 판단 ID나 미확인 후보에 대한 승인은 거절된다. 사람 검수를 수행하지 않은 실행은 reviewer-kind에 human을 사용하지 않는다.
