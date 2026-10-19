# 기간별 아티스트 청취자 순위

> 이 문서는 이전 상세 팩트를 입력으로 하는 일별 및 주별 구현이다. 최신 월별 Silver용 Gold는 보류 중이며 [현재 계획](status.md)에 따라 별도로 구현한다.

`gold/mart_period_artist`는 UTC 청취 시각 기준의 일별과 주별 아티스트 순위다. 반복 청취를 삭제하지 않으며 청취 수와 서로 다른 청취 계정 수를 함께 보존한다. 기존 일별 마트와 청취 수 증가율 기반 급상승 마트는 별도 지표로 유지한다.

## 단위와 지표

한 행의 키는 `(period, period_start, artist_mbid)`다. `period`는 `day` 또는 `week`, 주의 시작은 ISO 기준 월요일이다. `period_start`는 포함하고 `period_end`는 포함하지 않는다. 예를 들어 2026-12-28 주는 UTC 2026-12-28 00:00부터 2027-01-04 00:00 직전까지다. 덤프 도착 날짜를 청취 날짜로 쓰지 않는다.

| 컬럼 | 의미 |
|---|---|
| `listens` | 해당 아티스트의 청취 횟수. 반복 2회는 2로 보존 |
| `listeners` | 해당 기간에 들은 서로 다른 `user_id` 수 |
| `top_user_listens` | 한 계정이 기록한 최대 청취 횟수 |
| `top_user_share` | `top_user_listens / listens`, 0부터 1까지의 비율 |
| `rank` | 청취자 수 내림차순, 청취 수 내림차순, 아티스트 MBID 오름차순의 고유 순번 |
| `period_listens` | 아티스트 연결 여부와 관계없는 기간 전체의 고유 청취 수 |
| `linked_listens` | 아티스트 MBID가 하나 이상 있는 기간 전체의 고유 청취 수 |
| `coverage` | `linked_listens / period_listens`, 0부터 1까지 |
| `period_users` | 해당 기간 팩트 전체의 서로 다른 계정 수 |
| `linked_users` | 해당 기간 아티스트 연결 청취가 있는 서로 다른 계정 수 |
| `_built_at` | 마트 생성 시각 |

기간 전체 지표는 그 기간의 각 아티스트 행에 같은 값이 반복된다. **아티스트별로 더하지 않는다.** 협업곡은 각 아티스트 청취에 한 번씩 기여하지만, 기간 전체의 고유 청취는 `(_source_dump, _record_id)`로 한 번만 센다. 팩트의 청취+아티스트 키 유일성은 기존 팩트 계약을 따른다.

주간 청취자 수는 일별 청취자 수의 합이 아니다. 같은 계정이 월요일과 화요일에 들으면 각 날짜에서는 한 명이지만 그 주에서는 한 명이다. 마찬가지로 여러 곡과 MSID에 걸친 청취도 같은 아티스트의 같은 기간에서는 한 계정이다. 최대 계정 비중도 기간 전체의 계정별 횟수에서 계산하며 일별 최대치를 합치지 않는다. 계정 ID 자체는 마트에 저장하지 않는다.

이름 대신 MBID를 저장한다. 이름이 필요하면 차원과 목적에 맞는 시점으로 연결한다. 아티스트 MBID가 없는 청취는 아티스트 순위에서 제외하지만 기간 커버리지의 분모에 포함한다. 연결 아티스트가 전혀 없는 기간은 아티스트 행이 없으므로 이 표에 나타나지 않는다.

## 생성과 갱신

```bash
./scripts/spark_run.sh -- python scripts/pipeline/gold_mart_periods.py
```

최신 팩트 버전을 한 번 고정한 뒤 `versionAsOf`로 읽어 마트 전체를 재계산한다. 특정 팩트 버전이나 별도 실험 출력은 다음처럼 지정한다.

```bash
./scripts/spark_run.sh -- python scripts/pipeline/gold_mart_periods.py \
  --fact-version 0 \
  --output file:/data/scratch/artist-periods/mart_period_artist
```

`--fact-path`로 별도 팩트도 지정할 수 있다. 원본과 출력이 같거나 서로의 하위 경로이면 거부한다. 출력은 `period`로 파티션하며, Delta 한 번의 전체 덮어쓰기 트랜잭션으로 교체한다. 기존 팩트와 다른 마트는 수정하지 않는다. 팩트 경로, 표 ID, 버전, UTC와 정렬 규칙 및 생성 시각을 출력 Delta 커밋의 `userMetadata`에 남긴다.

처음에는 정확성을 위해 전체 갱신을 사용한다. 과거 시각의 청취가 늦게 들어오거나 기존 청취가 수정·삭제되면 다음 갱신에서 해당 과거 기간과 순위도 바뀐다. 같은 팩트 버전을 다시 실행하면 생성 시각과 물리 파일은 달라도 집계 값은 같다. 마지막 청취가 삭제된 기간과 아티스트의 옛 행도 제거되며, 입력이 비면 마트도 빈 표로 교체한다. 고유 청취자 수를 기존 숫자에 단순 가산하는 방식은 사용하지 않는다. 향후 증분 최적화에서는 변경 전후가 걸친 일자와 주 전체를 재계산해야 한다.

## 조회

읽기 전용 DuckDB 질의 도구에 `{mart_period}`를 추가했다. 예를 들어 최신 주의 상위 20개는 다음과 같이 조회한다.

```sql
select period_start, rank, artist_mbid, listens, listeners,
       top_user_listens, round(top_user_share * 100, 2) as top_user_pct,
       round(coverage * 100, 2) as coverage_pct
from {mart_period}
where period = 'week'
  and period_start = (select max(period_start) from {mart_period} where period = 'week')
order by rank
limit 20;
```

## 검증과 적용 상태

2026-10-18 일반 테스트 751개 통과, 새 Spark 테스트 5개 통과(92.73초), 기존 급상승 Spark 회귀 2개 통과를 확인했다. 일반 테스트의 10개 건너뜀과 12개 제외는 Spark 및 외부 환경 의존 검증과 구분한다.

`tests/test_spark_artist_periods.py`는 반복 보존, 여러 날짜에 걸친 계정의 주간 고유 수, 청취자 수 우선 순위와 두 단계 동률 처리, 최대 계정 비중, 협업곡 커버리지, 아티스트 미연결 청취, UTC 오프셋과 연말·연초 주 경계를 검사한다. 임시 Delta 표에서 저장, 재실행, 지정 버전 재현, 과거 청취 추가와 기존 행 수정·삭제, 빈 입력 교체 및 출처 메타데이터도 검사한다. 운영 데이터를 연결하지 않는 작은 Spark 테스트다.

2026-10-19 30초 기준 Silver와 종속 집계를 별도 경로에서 재생성했고, 이 마트 22,229,206행을 독립 전수 대조했다. 운영 경로는 교체하지 않았다. 실행 결과와 예비 순위 비교는 [전체 재생성 결과](rebuild_30seconds.md)에 있다. 계정 수는 사람 수가 아니며 여러 계정을 만든 동일인을 구분할 수 없다. 집중도만으로 조작, 팬덤 또는 국가별 대중성을 확정하지 않는다.
