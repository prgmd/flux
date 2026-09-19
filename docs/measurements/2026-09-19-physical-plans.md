# 물리 계획 확인 — 스큐는 어디에 있나 (2026-09-19)

스큐 실험을 다시 하기로 한 이유를 계획에서 확인했다.

첫 실험(`docs/measurements/2026-09-18-skew-experiment.md`)이 `groupBy` 집계를
재서 파티션 수준 스큐가 **1.3배**밖에 안 나왔다. 키 수준은 500,143배였다.
"해시가 흩어 준다"로 설명했는데 그것만으로는 부족했다.

**계획을 뽑아 보니 답이 있었다.**

## 연산자 수

| 작업 | `HashAggregate` | `Exchange` | 조인 |
| --- | ---: | ---: | --- |
| `groupBy` 마트 | **8** | 4 | — |
| `groupBy` + 소금 | 8 | 4 | — |
| 이름 조인 (브로드캐스트) | 8 | 6 | `BroadcastHashJoin` |
| 이름 조인 (셔플) | 8 | 8 | `SortMergeJoin` |
| **팩트 직접 조인** | **0** | 4 | **`SortMergeJoin`** |

## `groupBy`는 셔플 전에 접는다

`HashAggregate`가 8개다. **부분 집계와 최종 집계가 쌍으로 있다는 뜻**이고,
그 사이에 `Exchange`(셔플)가 끼어 있다.

```
HashAggregate   ← 최종 집계
  Exchange      ← 셔플
    HashAggregate   ← 부분 집계 (map-side combine)
      Scan
```

Spark는 입력 파티션 안에서 먼저 `(artist_mbid, listened_date)`로 묶은 뒤
**그 결과만** 셔플한다. BTS의 300만 행이 569개 입력 태스크에 흩어져 있으면,
태스크마다 날짜별로 접혀서 수천 행이 되어 나간다.

**스큐가 셔플에 도달하기 전에 사라진다.** 그러니 그 위에서 AQE와 Salting을
비교한 것은 병목이 없는 곳에서 병목 처리 기법을 비교한 것이다.

`countDistinct`가 이것을 더 심하게 만든다. Spark는 distinct 집계를 위해
1단계 셔플 키에 distinct 대상 컬럼을 넣는다. `(artist, date, listen_key)`로
셔플하는데 `listen_key`는 행마다 거의 유일하다. **스큐가 구조적으로 존재할
수 없는 키였다.**

## 팩트 직접 조인만 접는 것이 없다

```
== Physical Plan ==
AdaptiveSparkPlan
+- Project
   +- SortMergeJoin LeftOuter
      :- Sort
      :  +- Exchange          ← 여기서 BTS 300만 행이 한 파티션으로
      :     +- Filter
      :        +- Scan parquet
```

`HashAggregate`가 **0개**다. `Scan → Filter → Exchange → Sort → Join`으로
끝난다. 조인은 같은 키의 행을 한 곳에 모아야 하므로 **미리 접을 수 없다.**

`fact_listens`의 아티스트 있는 줄 6,060만 행 중 BTS가 300만 행이다. 그 300만
행이 `Exchange`를 지나 **한 파티션에 통째로** 떨어진다. 셔플 파티션 200개에
평균 30만 행인데 한 파티션만 300만 행이므로 **약 10배**다.

여기가 실험 대상이다.

## 이름 조인은 대상이 아니다

이름 조인(`with_names`)은 `HashAggregate` 8개를 달고 있다. **집계 결과에**
차원을 붙이기 때문이다. 마트가 1,479만 행이고 BTS는 그중 날짜 수만큼
(12년치라 4천여 행)뿐이라 스큐가 없다.

집계 뒤에 이름을 붙이는 것은 설계상 맞다 — 1.5억 줄에 조인하는 대신
아티스트-날짜 조합에만 붙인다. **다만 그래서 스큐 실험의 대상이 못 된다.**

브로드캐스트를 강제하면 `BroadcastHashJoin`이 되어 셔플 자체가 없다.
`--no-broadcast`를 붙이면 `SortMergeJoin`으로 바뀌는 것도 계획에서 확인했다.

## 그래서 바꾸는 것

실험 작업을 **팩트를 차원에 직접 조인하는 것**으로 바꾼다.

```python
fact.filter(artist_mbid.isNotNull())
    .join(dim.select(artist_mbid, name), on="artist_mbid", how="left")
```

소금 경로는 조인 양쪽에 친다. 큰 쪽(팩트)에는 임의 소금을, 작은 쪽(차원)에는
소금 가짓수만큼 복제해 붙인다. 집계와 달리 **조인은 반대쪽도 같은 소금을
갖고 있어야 짝이 맞는다.**

기존 `groupBy` 결과는 버리지 않는다. **"groupBy에서는 부분 집계가 스큐를
지운다"**는 것도 실측이고, 오히려 더 쓸모 있는 발견이다.

## 여기서 배운 것

**재는 작업이 재려는 현상을 지우고 있지 않은지 먼저 봐야 한다.**

부분 집계는 Spark가 알아서 하는 최적화라 코드에 안 보인다. `groupBy`를 쓰면
스큐가 날 것 같은데 실제로는 **`groupBy`가 스큐에 가장 강한 연산**이다.
스큐가 진짜로 문제 되는 자리는 조인과 윈도우 함수다. 둘 다 같은 키의 행을
한 곳에 모아야 하고 미리 접을 수 없다.

실험 전에 `explain()`을 한 번 봤으면 알 수 있었다. **성능을 재는 실험에는
물리 계획을 붙이는 것을 규칙으로 한다.**

## 재현

```
docker run --rm -m 10g -v <데이터 루트>:/data \
  -e FLUX_STORAGE_ROOT=file:///data ... flux-spark \
  python scripts/explain_plans.py
```

계획을 뽑을 뿐이라 데이터를 읽지 않는다. 1분 안에 끝난다.
원문은 데이터 루트의 `plans/*.txt`, 요약은 `measurements/explain-plans.json`.
