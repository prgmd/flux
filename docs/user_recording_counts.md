# 사용자별 곡 청취 횟수

Silver의 `user_recording_counts`는 날짜 구분 없이 `user_id + recording_mbid`당 한 행을 저장한다.

| 컬럼 | 의미 |
| --- | --- |
| `user_id` | 사용자 식별자. 이름이 바뀌어도 같은 사용자로 묶는다 |
| `recording_mbid` | 같은 녹음 음원을 묶는 MusicBrainz ID |
| `play_count` | 품질 검사와 동일 청취 중복 제거를 통과한 청취 횟수. bigint |

같은 사용자의 같은 MBID는 MSID가 달라도 합친다. 다른 사용자나 다른 MBID는 별개다. `recording_mbid`가 NULL인 기록은 집계하지 않으며 `listens_normalized`에 그대로 보존한다. MBID가 있는 데이터만의 집계이므로 전체 청취자 규모를 대표한다고 해석하지 않는다. 라이브와 리믹스 등 서로 다른 녹음은 별개다.

## 갱신

`silver_build.py`와 `silver_increment.py`는 상세 Silver 저장이 끝난 뒤 이 표를 갱신한다. 실제 재생 시간 `ms_played`가 숫자로 확인되고 10,000밀리초 미만인 행은 먼저 격리한다. 정확히 10초인 값과 시간이 없는 기록은 유지하며, 곡 길이인 `duration_ms`로 대체하지 않는다. Bronze 원문은 보존한다. 집계는 이미 동일 청취 중복을 제거한 `listens_normalized`에서 계산한다. 새 청취가 하나 늘면 해당 횟수가 하나 늘고, 처음 보는 사용자와 곡 조합은 새 행이 된다.

현재 구현은 최신 상세 Silver의 Delta 버전을 고정해 읽고 전체 횟수를 다시 계산해 Delta 트랜잭션 하나로 덮어쓴다. 이전 집계에 무조건 더하지 않으므로 같은 덤프 재처리나 집계 재시도에 횟수가 증가하지 않는다. MBID 수정과 청취 삭제도 다음 갱신 때 반영되며, 더 이상 청취가 없는 조합은 사라진다. 상세 Silver를 입력으로 받는 `aggregate()`에 Bronze나 중복이 남은 행을 직접 넘기지 않는다.

매번 상세 Silver 전체를 읽는 비용이 있다. 키별 증분 가산은 아직 구현하지 않았다. 이 방식은 단일 실행자를 전제로 하며 병렬 갱신을 실행하지 않는다. 상세와 집계는 각각 별도의 Delta 트랜잭션이므로 집계가 실패하면 상세만 최신일 수 있다. 이때 아래 명령으로 재집계한다. 실행 중 새로 반영된 상세 기록은 다음 갱신에서 포함된다.

```bash
./scripts/spark_run.sh -m 12g -- python scripts/pipeline/silver_user_recording_counts.py
```

이 명령은 기존 상세 Silver를 읽는다. 10초 미만 제외 규칙을 과거 데이터에 적용하려면 상세 Silver부터 다시 생성해야 한다. 코드 변경만으로 운영 데이터가 바뀌지는 않는다. 기존 Gold와 탐색 화면은 상세 기록 기반으로 유지되며 이 집계로 자동 전환되지 않는다.

## 조회

SQL 실행기에서 `{user_recording_counts}`로 읽을 수 있다.

```sql
SELECT recording_mbid,
       count(*) AS listener_count,
       sum(play_count) AS play_count
FROM {user_recording_counts}
GROUP BY recording_mbid;
```

한 곡 안에서는 행 수가 청취자 수다. 여러 곡이나 아티스트를 합칠 때는 같은 사용자가 다시 등장하므로 `count(DISTINCT user_id)`로 계산한다. 이 표는 반복 청취의 영향과 청취자 규모를 구분하게 하며, 반복 청취를 조작으로 판정하거나 삭제하지 않는다.
