# 월별과 연간 Gold

월별 Silver를 읽어 곡, 아티스트, 아티스트 안의 곡, 버전 유형과 계정별 요약을 만든다. 기준 시각은 UTC 청취 월이다. 연간 지표도 사용자별 월별 기록에서 다시 계산하며 월별 청취자 수를 더하지 않는다.

## 식별과 귀속 범위

공식 MBID, 제목과 크레딧이 대조된 일치 후보만 같은 공식 녹음 키로 연결한다. 나머지는 MSID별로 유지한다. 따라서 곡 표의 모든 행이 최종적으로 구분된 실제 곡이라는 뜻은 아니다. `identity_status`와 `source_msids`로 범위를 표시한다. `observed_artist`는 표시용 사전순 대표값이며 귀속 기준이 아니다. 공식 귀속은 별도 `song_credits`가 정하고 MSID별 관측값은 `song_dimension`과 원본 사전에 남긴다. 옛 ID 연결과 독립적인 아티스트 판정은 [귀속 분리 정책](measurements/2026-10-19-artist-attribution.md)을 따른다. 별칭과 제목 차이의 확대 검수는 후속이다.

아티스트는 별도 귀속 판정이 통과한 공식 크레딧 구성원에게 각각 횟수를 부여한다. 곡 버전 검토 상태여도 제목과 크레딧이 일치하면 귀속할 수 있다. 제작진 관계를 펼치지 않는다. 참여자 두 명이면 양쪽에 전부 집계되므로 아티스트별 횟수 합은 전체 청취 수와 다를 수 있다. 미확정 귀속은 곡 표에 보존하고 아티스트 표에서 제외하며 `coverage`에 귀속률을 표시한다. `identity_coverage`는 곡 식별 상태이며 귀속 상태와 다르다. 현재 확인 범위의 아티스트 순위이며 전체 ListenBrainz 아티스트 순위로 일반화하지 않는다.

같은 녹음으로 연결한 MSID의 횟수는 합한다. 다른 MSID에 중복 등록된 동일 시각 사건을 추가 차감하지 않는다. 아직 남아 있을 수 있는 사건 중복과 곡 연결을 구분한다.

## 버전 분류

`variant-label-v1`은 제목의 괄호, 대괄호와 공백 하이픈 뒤 문구를 읽는다. 제목과 아티스트가 공식 자료와 일치한 경우에만 공식 녹음 설명도 이용한다. 관측 제목 전체에서 단어를 무조건 찾지 않으므로 Live Forever를 제목만으로 라이브로 분류하지 않는다.

sped_up, slowed, remix, live, acoustic, instrumental, remaster, clean, explicit, edit, effect를 복합 분류한다. 예를 들어 `sped_up+remix`, `slowed+effect`, `live+remaster`를 하나의 조합으로 저장한다. 이 조합별 청취 합은 전체와 일치한다. 개별 태그별로 다시 펼치면 서로 겹칠 수 있어 태그별 수치를 더하지 않는다.

버전 문구를 지워 기본 제목에 합산하지 않는다. 아티스트 전체 지표에는 그 아티스트에 공식 귀속된 여러 버전의 실제 청취를 포함하되, 같은 계정이 여러 버전을 들어도 해당 기간의 아티스트 청취자는 한 명으로 센다. 버전을 분류한다는 이유로 실제 청취를 삭제하거나 가중치를 임의로 낮추지는 않는다. 현재 규칙이 인식하는 버전 표기가 없으면 `unmarked`이며 원곡 판정이 아니다. 이 범위에는 알려지지 않은 접미사가 있는 제목도 포함될 수 있다. 한 녹음 키의 분류가 서로 다르면 `conflicting_labels`로 남긴다. 접미사가 아닌 표기, 외국어와 공식 메타데이터 자체의 누락은 놓칠 수 있다. 월별 사전에 앨범명이 없으므로 앨범에만 붙은 [Explicit] 같은 표지는 이 분류에 사용하지 않는다. 분류는 관측된 텍스트 기반이고 오디오 속도나 실제 음원을 분석한 결과가 아니다. 버전 발매나 청취 패턴만으로 팬덤의 의도 또는 조작 여부를 추론하지 않는다.

## 마트와 지표

| 마트 | 단위 | 주요 지표 |
| --- | --- | --- |
| song_metrics | 기간+녹음 키 또는 미확정 MSID | 청취 수, 청취자 수, 두 기준의 순위, 최대 계정 비중, 계정당 청취 수, 집중도 |
| artist_metrics | 기간+공식 아티스트 | 동일 지표, 공식 귀속 범위 내 집계 |
| artist_song_metrics | 기간+아티스트+곡 | 아티스트 내 청취자 순위, 곡의 청취 비중, 청취자 도달 비율 |
| variant_metrics | 기간+버전 조합 | 청취 수, 청취자 수, 청취 비중, 청취자 도달 비율, 집중도 |
| listener_metrics | 기간+계정의 SHA-256 키 | 청취 수, 곡 키 수, 최다 청취 곡 비중 |
| coverage | 기간 | 전체 청취와 계정 수, 공식 귀속 청취와 계정 수, 귀속 커버리지 |
| identity_coverage | 기간+식별 검수 상태 | 상태별 청취와 고유 계정 수 |

계정 키 해시는 가명화이며 익명화 보장이 아니다. 계정별 마트는 로컬 분석용이고 저장 데이터를 git에 올리지 않는다. 여러 계정을 가진 한 사람을 구별할 수 없어 청취자 수는 고유 계정 수를 뜻한다.

순위는 `RANK`로 청취자 수만 비교한다. 동률은 공동 순위이고 다음 순위는 건너뛴다(1, 1, 3). 출력 순서는 순위 뒤 이름, 안정적인 키 순이다. 청취 수 순위도 비교용으로 남긴다. Parquet의 저장 순서에 의존하지 말고 조회 시 같은 ORDER BY를 사용한다.

`top_user_share`는 최대 계정 횟수/전체 횟수다. `concentration_hhi`는 계정별 청취 비중의 제곱합이고 `effective_listeners`는 그 역수다. 두 계정이 90%와 10%를 차지하면 집중도는 0.82, 환산값은 약 1.22다. 실제 사람이 1.22명이라는 뜻이 아니라 균등 분포의 계정 수에 대응하는 비교 지표다. 팬덤 규모나 부정 청취 판정으로 사용하지 않는다.

`song_listen_share`는 해당 아티스트의 확인된 청취 가운데 곡이 차지하는 비중이고 `song_listener_reach`는 해당 곡 청취자/아티스트 청취자다. 한 계정이 여러 곡을 들어도 되므로 도달 비율은 곡별로 더하면 1을 넘을 수 있다.

## 생성과 재검증

```powershell
$env:PYTHONPATH='src'
venv/Scripts/python scripts/build_monthly_gold.py --silver <월별Silver> --audit <공식크레딧조사본> --output <새Gold경로>
```

공식 조사본의 Silver 로그 지문이 현재 입력과 일치해야 실행한다. 출력은 기존 폴더를 덮어쓰지 않는 새 경로다. 월별과 연간 각각 일곱 Parquet 마트, 식별 사전 세 개와 완료 manifest를 만든다. 생성 중 DuckDB는 계산용 사본이며, 최종화 단계에서 별도 경로에 읽기용 스냅샷을 만든다. 이번 Gold는 DuckDB 배치로 생성한 불변 Parquet 스냅샷이며 Delta 증분 마트는 아니다. Silver는 Delta로 유지한다. 계산용 DuckDB의 요약 테이블은 마지막 생성한 연간 결과다. 최종화된 조회용 gold.duckdb는 month_song_metrics, year_song_metrics처럼 기간 접두사가 있는 뷰로 양쪽 결과를 제공하며 데이터를 DB에 다시 복제하지 않는다.

`result.json`의 completed가 true이고 입력 지문 불변 검증이 끝난 경우에만 완성본으로 사용한다. 중간 실패 폴더를 완료로 사용하지 않는다. 다음 Silver 버전 또는 연결 규칙 변경 시 새 출력 경로에서 재생성하고 검증한 뒤 조회 대상을 바꾼다. 운영 승격과 Gold 증분 갱신은 별도 작업이다.

테스트는 반복 월의 동일 계정, 같은 곡의 여러 MSID, 다중 참여자와 중복 크레딧, 공동 순위, 집중도 공식, 버전 분리, 누락 식별 보존과 오래된 조사본 차단을 확인한다. 전체 데이터에서는 청취 합계, 내보낸 행 수, 입력 지문 불변과 실제 표본의 독립 재계산을 확인한다.


## 조회 예시

청취자 수 순위와 청취 수 순위를 나란히 본다. 아래 날짜는 조회할 청취 월로 바꾼다. 필터링 전에 전체 범위에서 순위를 계산해 저장했으므로 청취자 하한을 걸어도 순위를 새로 만들지 않는다.

```sql
SELECT artist_name, listens, listeners, listener_rank, listen_rank,
       round(100 * top_user_share, 2) AS top_account_percent,
       effective_listeners
FROM read_parquet('<Gold>/month/artist_metrics.parquet')
WHERE period_start = DATE '2026-08-01' AND listeners >= 10
ORDER BY listener_rank, artist_name, artist_mbid;
```

버전별 청취 흐름은 조합 단위를 그대로 비교한다.

```sql
SELECT period_start, variant, listens, listeners, listen_share, top_user_share
FROM read_parquet('<Gold>/month/variant_metrics.parquet')
WHERE variant <> 'unmarked'
ORDER BY period_start, listens DESC, variant;
```

연간은 경로의 month를 year로 바꾸고 period_start를 해당 연도 1월 1일로 조회한다. 순위를 읽을 때 같은 경로의 coverage를 함께 조회한다. 연결률이 낮은 기간의 순위 변화를 음악 시장 전체의 변화로 해석하지 않는다.


### 최종 스냅샷과 독립 표본 검수

```powershell
venv/Scripts/python scripts/finalize_monthly_gold.py --source <계산출력> --output <새최종Gold경로>
venv/Scripts/python scripts/verify_monthly_gold.py --silver <월별Silver> --audit <공식크레딧조사본> --gold <최종Gold경로> --song-mbid c73941d9-7ac1-486d-bd88-3d3f8e944a7d --artist-mbid 58f1e354-ecbc-4fd3-aa25-97eb1d9bd8b9
```

최종화는 횟수 및 순위를 BIGINT로 고정하고 모든 횟수 컬럼의 합과 행 수가 같음을 확인한다. 초기 실행에서 DuckDB의 HUGEINT 합계를 Parquet으로 내보낼 때 DOUBLE로 저장되는 것을 발견해 집계 코드에도 명시적 BIGINT를 추가했다. 초기 출력은 소수부가 없고 정확하게 표현 가능한 범위인지 검사한 뒤 별도 경로에서 정수형으로 저장한다. 값 반올림으로 오류를 숨기지 않는다.

검수 도구는 같은 Silver 버전과 같은 공식 조사본인지 확인한다. 선택한 곡 및 아티스트의 Silver 기록을 Python 사전으로 직접 누적해 월별과 연간 청취 수, 계정 수, 최대 계정 횟수와 집중도를 비교한다. 표본이 없는 경우는 성공으로 보고하지 않는다. 실행 시작에 검수 완료 표시를 해제하고 성공한 경우에만 다시 완료로 쓴다. 단위 테스트에서는 횟수를 일부러 바꾼 파일과 다른 조사본도 거부하는 것을 확인했다.


실제 생성 행 수, 검수 결과와 RM 및 Radiohead의 순위 비교는 [2026-10-19 실측](measurements/2026-10-19-monthly-gold.md)에 정리했다.
