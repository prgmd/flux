# 30초 기준 전체 재생성

2026-10-19 현재 운영 데이터를 교체하지 않고 별도 사본을 생성·검증하는 절차다. 실제 실행 결과는 검증 완료 후 보완한다.

## 입력과 출력

- 입력: 운영 Bronze `raw_listens` v0, 155,713,649행, 293개 파일, 27,786,350,920바이트. 기존 31개 덤프만 포함하며 추가 두 덤프의 실험 사본을 합치지 않는다.
- 운영 비교 기준: Silver v0 151,192,624행, 팩트 v0, 아티스트 차원 v8. 실행 전후의 표 ID·버전·전체 JSON 커밋 로그 해시를 기록한다.
- 별도 결과: `flux-data/rebuild-30s-20261019`. 운영 표 교체나 VACUUM은 하지 않는다.
- 실행은 전용 Docker 볼륨에서 진행하고 완료한 표를 위 호스트 경로에 내보낸다. 운영 경로는 컨테이너에 읽기 전용으로 연결한다.

먼저 시스템과 화면 절전 방지를 설정한다. 작업 종료 시 해제한다. 불변 Bronze 사본은 `scripts/drills/cache_silver_readonly.py`로 파일별 크기와 SHA-256을 대조한 뒤 사용한다. 아티스트 차원은 기존 파일과 로그 이력을 사본으로 준비한다.

## 생성

`scripts/migrations/rebuild_playback.py`는 기존 생산 함수를 다음 순서로 실행한다.

1. 품질 격리(숫자로 확인된 재생 시간 30초 미만 포함) 후 동일 청취 중복 제거를 거쳐 Silver와 격리 표 생성.
2. CDF가 활성화된 새 Silver에서 `user_recording_counts`를 `rebuild=True`로 초기화.
3. Gold 팩트 생성 및 복사한 아티스트 차원의 필요한 자리 행 보강.
4. 날짜·사용자 차원 재생성.
5. 기존 일별 마트와 청취 수 증가율 기반 주간 급상승 생성.
6. 새 일별·주별 청취자 순위 및 최대 계정 비중 마트 생성.

재생 시간이 없거나 숫자로 해석되지 않는 기록은 짧은 재생 사유로 격리하지 않는다. 원본의 30초 미만 건수와 Silver 감소량은 중복 제거 관계 때문에 반드시 같지 않다. 월별 사용자 이름 해시 16버킷 배치와 CDF를 유지한다. 새 원본 ID에 옛 집계의 처리 위치를 재사용하지 않는다.

컨테이너에서 `/production`은 운영 읽기 전용, `/work`는 검증한 Bronze·차원 사본과 출력용 볼륨, `/export`는 호스트의 별도 결과 경로로 연결한다.

```bash
python scripts/migrations/rebuild_playback.py \
  --production-root /production --work-root /work --export-root /export
```

단계별 완료와 표 ID·버전·로그 해시 및 경과 시간을 `rebuild.json`에 원자적으로 기록한다. 같은 볼륨으로 재실행하면 완료한 표의 상태를 확인한 후 다음 단계부터 이어간다. 기록된 완료 표가 달라졌다면 멈춘다. 완료 표가 아직 호스트에 전부 복사되지 않았더라도 재실행 시 다시 내보낼 수 있다. 한 결과 경로에는 동시에 여러 생성 작업을 실행하지 않는다.

## 독립 검증

예전 `compare_rebuild.py`는 기록 번호만 바뀌는 이전 작업용으로 내용과 행 수 동일을 요구한다. 이번에는 정책 변화로 행 수 감소가 정상이므로 그 조건을 그대로 적용하지 않는다.

`scripts/migrations/verify_playback_rebuild.py`는 호스트 DuckDB로 운영 Bronze에서 품질 사유와 남아야 할 후보를 독립 계산한다. 품질 검사를 먼저 한 뒤 출처·아티스트 수·덤프·기록 번호의 우선순위로 승자를 고른다. 예상 승자와 실제 Silver의 모든 출처 키 및 사용자·곡·청취 시각·MBID 선택·아티스트 배열을 대조한다. 전체 Silver 컬럼에 대한 바이트 단위 비교를 뜻하지는 않는다.

이어 다음을 전수 대조한다.

- 모든 격리 키와 사유, 모든 사용자+MSID별 횟수와 중복 키.
- Silver를 펼친 예상 팩트의 청취+아티스트 키와 팩트 값.
- 일별 합계·고유 청취 사건 수·시점 이름과 주간 급상승.
- 기간별 청취 수·고유 계정 수·순위·최대 계정 횟수와 비중 및 커버리지.
- 날짜 범위와 속성, 사용자별 이름·첫/마지막 청취·횟수.
- 고아 아티스트, NULL 식별자, Silver 중복 키와 한 사용자의 한 달이 여러 파일에 갈라졌는지.
- Silver CDF와 횟수 집계·팩트·새 마트의 원본 ID 및 처리 버전.

표마다 활성 파일 목록과 버전을 고정하고 종료 시 그대로인지 확인한다. 운영 원본의 로그와 ID도 재확인한다. 파일이 없는 정상적인 빈 Delta 표는 로그의 스키마로 읽는다. 실수로 옛 비활성 Parquet 파일을 함께 읽지 않는다.

```bash
venv/Scripts/python scripts/migrations/verify_playback_rebuild.py \
  --production-root C:/Users/SSAFY/Desktop/jjh/flux-data \
  --rebuild-root C:/Users/SSAFY/Desktop/jjh/flux-data/rebuild-30s-20261019
```

`--prepare-only`로 원본의 예상 결과 계산을 생성과 병행할 수 있다. `oracle.duckdb`에는 독립 중간 결과, `verification-snapshots.json`에는 고정 입력, `verification.json`에는 각 대조 결과가 저장된다. 표 생성 완료와 검증 완료는 다르며 마지막 `ok: true`와 운영 불변 감사까지 확인해야 한다. 데이터를 저장소에 커밋하지 않는다.

## 도구 검증

경로 겹침과 로그 변경 탐지, 30초 경계·음수·누락·텍스트·불리언 처리, 잘못된 값·누락·중복 키 탐지 및 빈 Delta 읽기를 검사하는 순수 테스트를 추가했다. `tests/test_spark_playback_rebuild.py`는 임시 Bronze에서 전체 생성과 재개를 실행하고 내보낸 모든 표를 DuckDB 검증기로 대조한다. 격리될 높은 우선순위 후보가 정상 후보를 밀어내지 않는지도 검사한다.

```bash
venv/Scripts/python -m pytest tests/test_rebuild_playback.py tests/test_playback_rebuild_oracle.py -q
MSYS_NO_PATHCONV=1 docker run --rm \
  --mount "type=bind,source=$(pwd -W),target=/opt/flux,readonly" \
  flux-spark:latest python -m pytest tests/test_spark_playback_rebuild.py -m spark -q -p no:cacheprovider
```

개발 중에는 컨테이너에 현재 소스와 테스트를 읽기 전용으로 마운트해 이미지에 남은 옛 코드가 실행되지 않게 한다. 전체 재생성 전후 수치가 달라졌다는 사실만으로 성능 개선이나 대표성 개선을 주장하지 않는다.
