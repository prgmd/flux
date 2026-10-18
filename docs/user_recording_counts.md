# 사용자별 곡 청취 횟수

Silver의 `user_recording_counts`는 날짜 구분 없이 `user_id + recording_mbid`당 한 행을 저장한다.

| 컬럼 | 의미 |
| --- | --- |
| `user_id` | 사용자 식별자. 이름이 바뀌어도 같은 사용자로 묶는다 |
| `recording_mbid` | 같은 녹음 음원을 묶는 MusicBrainz ID |
| `play_count` | 품질 검사와 동일 청취 중복 제거를 통과한 청취 횟수. bigint |

같은 사용자의 같은 MBID는 MSID가 달라도 합친다. 다른 사용자나 다른 MBID는 별개다. `recording_mbid`가 NULL인 기록은 집계하지 않으며 `listens_normalized`에 그대로 보존한다. MBID가 있는 데이터만의 집계이므로 전체 청취자 규모를 대표한다고 해석하지 않는다. 기존 상세 Silver 전수 실측에서 recording MBID 보유는 53,692,180행(35.51%), 미보유는 97,500,444행(64.49%)이었다. MSID의 전수 존재와 MBID의 보유율은 다르다. MSID가 다르더라도 같은 MBID가 연결되면 합치지만, 잘못 연결된 MBID를 이 단계가 교정하지는 않는다. 라이브와 리믹스 등 서로 다른 녹음은 별개다.

## 갱신

`silver_build.py`와 `silver_increment.py`는 상세 Silver 저장이 끝난 뒤 이 표를 갱신한다. 실제 재생 시간 `ms_played`가 숫자로 확인되고 10,000밀리초 미만인 행은 먼저 격리한다. 정확히 10초인 값과 시간이 없는 기록은 유지하며, 곡 길이인 `duration_ms`로 대체하지 않는다. Bronze 원문은 보존한다. 집계는 이미 동일 청취 중복을 제거한 `listens_normalized`에서 계산한다. 새 청취가 하나 늘면 해당 횟수가 하나 늘고, 처음 보는 사용자와 곡 조합은 새 행이 된다.

최초 생성 때는 상세 Silver 전체를 한 번 집계한다. 이후에는 마지막으로 처리한 Silver 버전 다음부터 최신 버전까지의 Delta Change Data Feed(CDF)를 읽는다. CDF는 삽입, 삭제와 수정 전후의 행을 구분한다.

- 삽입과 수정 후 행은 해당 사용자와 MBID에 +1을 반영한다.
- 삭제와 수정 전 행은 -1을 반영한다.
- 변경분을 사용자와 MBID별로 합산해 기존 행에는 가감하고, 새 조합은 생성한다. 합계가 0인 조합은 삭제한다.
- MBID가 없는 행은 가감 대상에서 제외한다. MBID가 채워지거나 빠지는 수정은 각각 새 그룹 가산과 이전 그룹 차감으로 반영된다.

가감과 처리 위치를 같은 집계 Delta 커밋의 userMetadata에 저장한다. 커밋 전에 실패하면 둘 다 반영되지 않고, 커밋 직후 실패하면 재실행에서 처리한 버전을 건너뛴다. 횟수 변화가 없는 변경도 메타데이터 커밋으로 처리 위치를 전진시킨다. 원본 표의 고유 ID도 저장해 다른 표의 변경 기록을 잘못 이어 읽지 않는다. 기존 Spark 세션의 커밋 메타데이터 설정은 실행 후 복원한다.

증분 집계에서는 상세 Silver 전체를 다시 집계하지 않는다. CDF 변경분을 읽고 집계 대상과 MERGE한다. 대상 파일의 탐색과 재작성 비용은 있으며 전체 집계 표를 전혀 읽지 않는다는 뜻은 아니다. 단일 실행자를 전제로 하며 같은 집계에 대한 병렬 갱신은 지원하지 않는다.

상세와 집계는 각각 별도의 Delta 트랜잭션이므로 집계가 실패하면 상세만 최신일 수 있다. 아래 명령은 마지막 처리 위치에서 이어 간다. 운영 Silver에는 CDF가 활성화되어 있어야 한다.

```bash
./scripts/spark_run.sh -m 12g -- python scripts/pipeline/silver_user_recording_counts.py
```

처리 위치가 없는 옛 집계, 교체된 원본, 보존 기간이 지나 사라진 CDF는 자동으로 전체 재집계하지 않고 오류로 알린다. 이 경우 명시적으로 초기화한다. 전체 Silver 생성 스크립트도 집계를 초기화한다.

```bash
./scripts/spark_run.sh -m 12g -- python scripts/pipeline/silver_user_recording_counts.py --rebuild
```

이 명령은 기존 상세 Silver를 원본으로 사용한다. 10초 미만 제외 규칙을 과거 데이터에 적용하려면 상세 Silver부터 다시 생성해야 한다. 코드 변경만으로 운영 데이터가 바뀌지는 않는다. 기존 Gold와 탐색 화면은 상세 기록 기반으로 유지되며 이 집계로 자동 전환되지 않는다.

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
