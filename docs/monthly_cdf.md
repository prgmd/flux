# 월별 Silver의 CDF 증분 검증

## 격리 범위

이 실험은 운영 Bronze나 Silver를 마운트하지 않는다. 고정 Bronze 원문 표본과 보관한 연속 두 덤프의 앞부분만 새 실험 폴더로 복사한다. 원본 압축 파일은 SHA-256을 대조하고 읽기만 한다. Spark에는 저장소의 소스와 스크립트를 읽기 전용으로, 실험 폴더만 쓰기 가능하게 연결한다. 기존 서비스의 컨테이너를 재시작하지 않으며 실험은 CPU 2개와 메모리 6GiB로 제한한다.

단일 작성자의 로컬 파일 시스템 실험이다. 분산 저장소의 잠금이나 동시 작성자 간 조정 및 전원 상실까지 검증한 결과가 아니다.

작은 표본의 동작 검증이며 새 덤프 전체의 처리 성능이나 7일 연속 운영 검증과 구분한다. 월별 빈도 표본 6,469행에서 상세 사건을 복원하지 않는다. 별도로 보관한 Bronze 원문 표본 256행과 실제 덤프의 원문을 사용한다.

## 비교 방법

기존 monthly_listens의 changes, affected와 refresh를 실행한다. CDF는 바뀐 사건을 찾는 데 사용하며 행 수를 play_count에 직접 더하지 않는다. 직전 처리 버전과 이번 종료 버전의 사건별 승자를 다시 계산하고 이전 기여를 빼고 이후 기여를 더한다. 품질 격리를 먼저 적용하고 사용자+MSID+동일 시각의 중복을 제거한 뒤 사용자+MSID+UTC 청취 월로 묶는다.

매 단계 Bronze 종료 버전을 고정하고 Silver 세 표의 체크포인트가 같은 원본 ID와 종료 버전을 가리키는지 확인한다. 그 버전의 원본 전체를 다시 처리한 Spark 결과 및 Python 기대값과 모든 월별 키 및 횟수를 대조한다. Python은 기존 품질과 MBID 해석 및 승자 선택 규칙을 재사용하되 집계는 별도로 계산한다. 동일한 업무 규칙을 사용하므로 음악 식별 정책의 사실 정확성을 독립적으로 증명하는 검사는 아니다.

격리는 현재 원본의 행 식별자, 사유와 원문을 대조한다. 사전은 관측한 변형의 누적 이력이므로 삭제 후 남은 과거 변형을 오류로 취급하지 않는다. 대신 현재 승자들의 변형 누락과 사전의 중복이 없어야 한다. 사전 전체를 현재 원본으로 만든 사전과 완전히 같게 요구하지 않는다.

## 시나리오와 실패 조건

처음에는 CDF 없는 복사본에서 전체 월별 결과를 만든다. 다음 덤프 쓰기 전에 사본의 CDF와 로그 및 삭제 파일 보존을 30일로 설정한다. 이 기간은 실험용이며 운영의 최대 중단 및 복구 기간을 검증해 확정한 값은 아니다. 활성화 전 변경이 소급 기록되지 않으며 필요한 파일은 VACUUM으로 제거하지 않는다. [Delta Lake CDF 문서](https://docs.delta.io/delta-change-data-feed/)를 따른다.

그 뒤 두 실제 덤프 표본과 각각의 재적재, 새 버전 없는 재실행, 같은 사건의 다른 덤프 재도착을 검사한다. 합성 행으로 사용자와 MSID 및 월과 연도를 바꾸는 UPDATE, 승자 DELETE 후 기존 후보 복원, 30초 경계에서 정상과 격리 상태의 교환, 마지막 집계 키와 격리 행 삭제를 실행한다. 수정 전후는 실제 update_preimage와 update_postimage가 모두 기록됐는지 확인한다. 어느 단계든 키별 결과나 격리 및 체크포인트가 다르면 완료하지 않는다.

CDF를 제공하지 않는 새 변경 구간, 원본 ID 교체, 과거 정책과 원본보다 앞선 처리 위치는 별도 경로에서 거부되는지 확인한다. 실제 보존 기간을 기다려 만료시킨 실험은 포함하지 않는다. 원본 없이 계속 가산하거나 임의 전체 재생성을 시도하지 않는 것이 실패 처리다.

## 실제 프로세스 중단과 재개

마지막 새 청취의 월별 집계 커밋 직후, 보조 표를 갱신하기 전에 os._exit(86)으로 Python 프로세스를 종료한다. 예외의 finally 정리를 거치지 않으므로 작성자 잠금도 남는다. 세 표의 처리 위치가 다르면 pin_snapshot이 읽기를 거부해야 한다. 종료 전 입력 지문, PID, 표 버전과 대기 상태를 실험 폴더에 남긴다.

호스트에서 해당 컨테이너가 종료 코드 86으로 끝났는지 확인한 뒤에만 복구 명령에 확인 값을 전달한다. 실험의 PID와 잠금 내용 및 입력 지문이 같을 때 해당 잠금 파일 하나만 해제한다. 이는 기존 작성자가 종료됐음을 운영자가 확인하는 절차이며 임의의 오래된 잠금을 자동으로 푸는 기능이 아니다. 복구는 보조 표만 진전시키고 이미 커밋된 월별 표 버전을 다시 바꾸지 않아야 한다. 다시 실행해도 세 표 모두 무변경이어야 한다.

## 재현 명령

Git Bash에서 저장소 루트를 기준으로 새 폴더 이름을 사용한다. 첫 명령은 호스트 Python 3.14의 표준 zstd 압축 모듈을 사용한다. 파일을 다시 받는 명령은 아니다.

```bash
PYTHONPATH=src venv/Scripts/python scripts/drills/prepare_monthly_cdf.py \
  --baseline data/governance-baseline-20261022-r2 \
  --output data/monthly-cdf-new --limit 128

export MSYS_NO_PATHCONV=1
repo="$(pwd -W)"
experiment="${repo}/data/monthly-cdf-new"
docker run --name flux-cdf-new --cpus 2 --memory 6g \
  -v "${repo}/src:/opt/flux/src:ro" \
  -v "${repo}/scripts:/opt/flux/scripts:ro" \
  -v "${experiment}:/experiment" \
  -v flux-ivy2:/opt/flux/ivy \
  flux-spark python scripts/drills/monthly_cdf.py --root /experiment

docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' flux-cdf-new
```

위 출력이 `exited 86`일 때 복구한다. 다른 종료 코드라면 원인을 먼저 조사하며 잠금을 해제하지 않는다.

```bash
docker run --rm --cpus 2 --memory 6g \
  -v "${repo}/src:/opt/flux/src:ro" \
  -v "${repo}/scripts:/opt/flux/scripts:ro" \
  -v "${experiment}:/experiment" \
  -v flux-ivy2:/opt/flux/ivy \
  flux-spark python scripts/drills/monthly_cdf.py --root /experiment \
  --recover --confirmed-exit-code 86
```

result.json의 completed가 true일 때만 전체 실험 완료다. progress 파일이나 중단 표식만으로 성공이라고 판단하지 않는다. 실행 시간은 refresh 구간의 실제 경과 시간이며 전체 비교 및 Spark 기동 비용과 구분한다. Spark 이벤트 로그에는 작업별 입력 레코드와 바이트 및 셔플 지표를 남긴다. 작게 제한한 실험의 결과를 전체 규모의 처리 시간으로 환산하지 않는다.
