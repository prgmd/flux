# 사용자별 곡 청취 횟수

Silver의 `user_recording_counts`는 날짜 구분 없이 `user_id + recording_msid`당 한 행을 저장한다.

| 컬럼 | 의미 |
| --- | --- |
| `user_id` | 사용자 식별자. 이름이 바뀌어도 같은 사용자로 묶는다 |
| `recording_msid` | ListenBrainz의 제출된 곡 정보 식별자 |
| `play_count` | 품질 검사와 동일 청취 중복 제거를 통과한 청취 횟수. bigint |

같은 사용자의 같은 MSID를 합친다. 다른 사용자나 다른 MSID는 별개다. MBID 유무를 조건으로 거르지 않으므로 정제와 동일 청취 중복 제거를 통과한 모든 상세 행이 집계에 포함된다. MBID 컬럼 자체가 없어도 계산할 수 있다. 기존 MSID 전수 비결측 실측을 전제로 하며 MSID가 누락된 입력을 위한 임의 식별자 생성 규칙은 두지 않는다.

MBID와 곡명 등의 메타데이터는 상세 `listens_normalized`에 보존한다. 같은 MSID에서 MBID만 변경되면 횟수는 그대로다. 서로 다른 MSID는 같은 MBID가 연결되어 있더라도 현재 집계에서 합치지 않는다. 같은 곡의 MSID들을 연결하는 판단은 후속 매칭 단계에서 별도로 설계한다. 이 표의 집계 단위는 녹음 자체가 아니라 제출된 곡 정보의 ID다.

## 갱신

`silver_build.py`와 `silver_increment.py`는 상세 Silver 저장이 끝난 뒤 이 표를 갱신한다. 실제 재생 시간 `ms_played`가 숫자로 확인되고 10,000밀리초 미만인 행은 먼저 격리한다. 정확히 10초인 값과 시간이 없는 기록은 유지하며, 곡 길이인 `duration_ms`로 대체하지 않는다. Bronze 원문은 보존한다. 집계는 이미 동일 청취 중복을 제거한 `listens_normalized`에서 계산한다. 새 청취가 하나 늘면 해당 횟수가 하나 늘고, 처음 보는 사용자와 곡 조합은 새 행이 된다.

최초 생성 때는 상세 Silver 전체를 한 번 집계한다. 이후에는 마지막으로 처리한 Silver 버전 다음부터 최신 버전까지의 Delta Change Data Feed(CDF)를 읽는다. CDF는 삽입, 삭제와 수정 전후의 행을 구분한다.

- 삽입과 수정 후 행은 해당 사용자와 MSID에 +1을 반영한다.
- 삭제와 수정 전 행은 -1을 반영한다.
- 변경분을 사용자와 MSID별로 합산해 기존 행에는 가감하고, 새 조합은 생성한다. 합계가 0인 조합은 삭제한다.
- MBID가 없는 행도 같은 방식으로 가감한다. MSID가 수정되면 이전 MSID에서 차감하고 새 MSID에 가산한다. MBID만 수정되면 그룹별 순변화량은 0이다.

가감과 처리 위치를 같은 집계 Delta 커밋의 userMetadata에 저장한다. 커밋 전에 실패하면 둘 다 반영되지 않고, 커밋 직후 실패하면 재실행에서 처리한 버전을 건너뛴다. 횟수 변화가 없는 변경도 메타데이터 커밋으로 처리 위치를 전진시킨다. 원본 표의 고유 ID와 집계 키도 저장해 다른 표나 다른 집계 단위의 처리 위치를 잘못 이어 읽지 않는다. 기존 Spark 세션의 커밋 메타데이터 설정은 실행 후 복원한다.

증분 집계에서는 상세 Silver 전체를 다시 집계하지 않는다. CDF 변경분을 읽고 집계 대상과 MERGE한다. 대상 파일의 탐색과 재작성 비용은 있으며 전체 집계 표를 전혀 읽지 않는다는 뜻은 아니다. 단일 실행자를 전제로 하며 같은 집계에 대한 병렬 갱신은 지원하지 않는다.

상세와 집계는 각각 별도의 Delta 트랜잭션이므로 집계가 실패하면 상세만 최신일 수 있다. 아래 명령은 마지막 처리 위치에서 이어 간다. 운영 Silver에는 CDF가 활성화되어 있어야 한다.

```bash
./scripts/spark_run.sh -m 12g -- python scripts/pipeline/silver_user_recording_counts.py
```

처리 위치가 없거나 집계 키가 다른 옛 집계, 교체된 원본, 보존 기간이 지나 사라진 CDF는 자동으로 전체 재집계하지 않고 오류로 알린다. 이 경우 명시적으로 초기화한다. MBID 기준으로 생성한 기존 표는 버전 숫자가 같아도 이어 쓸 수 없으며 아래 명령으로 MSID 기준으로 한 번 재생성해야 한다. 전체 Silver 생성 스크립트도 집계를 초기화한다.

```bash
./scripts/spark_run.sh -m 12g -- python scripts/pipeline/silver_user_recording_counts.py --rebuild
```

이 명령은 기존 상세 Silver를 원본으로 사용한다. 10초 미만 제외 규칙을 과거 데이터에 적용하려면 상세 Silver부터 다시 생성해야 한다. 코드 변경만으로 운영 데이터가 바뀌지는 않는다. 기존 Gold와 탐색 화면은 상세 기록 기반으로 유지되며 이 집계로 자동 전환되지 않는다.

## 조회

SQL 실행기에서 `{user_recording_counts}`로 읽을 수 있다.

```sql
SELECT recording_msid,
       count(*) AS listener_count,
       sum(play_count) AS play_count
FROM {user_recording_counts}
GROUP BY recording_msid;
```

한 MSID 안에서는 행 수가 청취자 수다. 실제 같은 곡이 여러 MSID로 나뉘어 있으면 행 수를 단순 합산하지 않고 후속 매칭 뒤 사용자의 고유 수를 계산해야 한다. 여러 곡이나 아티스트를 합칠 때는 같은 사용자가 다시 등장하므로 `count(DISTINCT user_id)`로 계산한다. 이 표는 반복 청취의 영향과 청취자 규모를 구분하게 하며, 반복 청취를 조작으로 판정하거나 삭제하지 않는다.
