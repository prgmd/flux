# broadcast 대 shuffle 조인 — 마트 이름 조인 (2026-10-02)

명세 6.5의 "broadcast vs shuffle join" 줄이다. 측정 지표는 실행 시간과 실행기 메모리다.

## 재는 작업

일별 아티스트 마트(14,792,567행)에 그날 유효했던 아티스트 이름을 붙이는 시점 조인이다
(`mart_build.with_names`). 차원은 2,976,554 버전, 그중 마트에 나오는 아티스트는 311,929명이다.

| 방식 | 무엇을 하나 |
| --- | --- |
| `shuffle` | 양쪽을 아티스트 키로 셔플해 정렬 병합한다(`SortMergeJoin`) |
| `broadcast` | 차원 전체를 해시 테이블로 만들어 실행기에 복사한다(`BroadcastHashJoin`) |
| `pruned` | 마트에 나오는 아티스트만 남긴 차원을 복사한다. 그 키를 알려고 마트를 한 번 더 읽는다 |

입력은 이미 만든 마트에서 이름 컬럼을 뗀 것이다. `gold_mart.py --no-broadcast`로 재면 집계와 마트
쓰기가 같이 돌아 시간에 섞이고 운영 마트를 덮어쓰므로, 이름 조인만 따로 도는 스크립트를 두었다
(`scripts/experiments/experiment_broadcast.py`).

| | |
| --- | --- |
| 환경 | 로컬 모드 `local[*]`, 20코어, 드라이버 8g |
| 반복 | 방식마다 3회, 회차를 바깥 루프에 |
| AQE | 켜짐(기본) |
| 메모리 측정 | 이벤트 로그의 실행기 지표, 100ms마다 측정(`spark.executor.metrics.pollingInterval`) |
| 결과 대조 | 세 방식 모두 14,792,567행, 이름 붙음 14,792,567, 해시 합 일치 |

## 결과

| 방식 | 중앙 소요 | 범위 | 셔플 | 저장 메모리 최고 | 실행 메모리 최고 | GC |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `shuffle` | 12.9초 | 12.2~23.1초 | **495.8MB** | 2.6MB | **2,063.6MB** | 472~624ms |
| `broadcast` | 12.0초 | 11.6~13.0초 | 0.1MB | **947.8MB** | 186.0MB | 295~331ms |
| `pruned` | 12.7초 | 12.7~13.6초 | 94.0MB | 183.3MB | 382.2MB | 251~352ms |

- **저장 메모리**: 캐시와 브로드캐스트 블록이 올라가는 자리. 차원을 복사하는 비용이 여기 찍힌다
- **실행 메모리**: 정렬과 해시 조인이 쓰는 자리. 정렬 병합의 버퍼가 여기 찍힌다
- 메모리 값은 세 런에서 거의 같았다(저장 947.7~947.8MB, 실행 2,063.6MB 세 번 동일)
- 힙 전체 최고값(4.4~5.6GB)은 GC 시점에 따라 흔들려 표에서 뺐다

최종 계획에서 의도한 조인이 실제로 돌았다(이벤트 로그의 `isFinalPlan=true` 계획, 아홉 런 전부).

```
shuffle    SortMergeJoin LeftOuter
             :- Exchange (마트)    +- Exchange (차원)
broadcast  BroadcastHashJoin LeftOuter BuildRight
             :- Scan (마트)        +- BroadcastExchange (차원)
pruned     BroadcastHashJoin LeftOuter BuildRight
             :- Scan (마트)
             +- BroadcastExchange
                  +- BroadcastHashJoin LeftSemi    ← 차원을 쓰이는 키로 거른다
                       :- Scan (차원)
                       +- BroadcastExchange
                            +- Exchange             ← 마트의 키를 distinct로 모은다
                                 +- Scan (마트)
```

## 시간은 갈리지 않았다

세 방식의 범위가 겹친다. 중앙값 차이가 0.9초 안이고, 런 사이 흔들림(`broadcast` 11.6~13.0초)과
같은 크기다.

`shuffle`의 최댓값 23.1초는 **첫 런**이다. 회차를 바깥 루프에 두어 첫 회차의 첫 방식이 늘 `shuffle`이었다.
PySpark는 세션을 닫고 새로 만들어도 같은 JVM을 계속 쓰므로 JIT 컴파일 결과가 다음 런에 이어진다.
첫 런만 차갑다. 2, 3회차의 `shuffle`은 12.9초, 12.2초였다.

**사전 확인(각 1회)에서는 `shuffle` 21.4초, `broadcast` 10.4초로 두 배 차이가 났다.** 그것도 `shuffle`이
첫 런이었기 때문이다. 한 번씩만 돌렸으면 "broadcast가 두 배 빠르다"고 기록했을 것이다.

로컬 모드라는 것도 시간 차이를 줄인다. 셔플은 네트워크를 타지 않고 같은 프로세스의 디스크를
거치며, 브로드캐스트는 실행기가 드라이버 자신이라 보낼 곳이 없다. 두 방식이 각자 가장 비싼 부분을
치르지 않는 환경이다.

## 갈린 것은 셔플과 메모리다

**`broadcast`는 셔플 496MB를 없애고 저장 메모리 948MB를 쓴다.** 디스크에서 228MB인 차원(그중 네
컬럼만 쓴다)이 해시 테이블로 펼쳐지면 그만큼 커진다.

**`shuffle`은 실행 메모리를 2GB 쓴다.** 양쪽을 정렬하는 버퍼다. 그래서 조인에 쓰인 메모리를 합치면
`shuffle`이 약 2.07GB로 가장 크고, `broadcast`가 약 1.13GB다. "브로드캐스트는 메모리를 많이 쓴다"는
말은 맞지만, 정렬 병합도 공짜가 아니라는 것이 같이 보인다.

**`pruned`는 메모리가 가장 적다**(저장 183MB, 실행 382MB, 합 약 0.57GB). 대가는 마트를 한 번 더
읽어 키를 모으는 셔플 94MB다. 차원이 9.5배 줄었는데(297만에서 31만 남짓) 저장 메모리는 5.2배만
줄었다. 해시 테이블에 행 수와 무관한 고정 비용이 있는 것으로 보이지만 확인하지 않았다.

## 클러스터라면 (추정)

로컬 결과로 클러스터를 짐작할 수 있는 것과 없는 것이 있다.

- **브로드캐스트는 실행기마다 사본을 둔다.** 그런데 이 저장소의 클러스터 실행기는 **통합 메모리(저장과
  실행을 합친 자리)가 456MB**다. 워커는 3GB를 내주지만 세션이 `spark.executor.memory`를 정하지 않아
  기본값 1g로 뜨기 때문이다(클러스터 스큐 실험의 이벤트 로그에서 확인). **948MB짜리 broadcast는 실행기에
  들어가지 않는다.** `pruned`의 183MB만 들어간다. 이 조인을 지금 설정 그대로 클러스터에서 돌리면 기본값인
  broadcast가 먼저 막힐 것이다
- **셔플은 네트워크를 탄다.** 클러스터 스큐 실험에서 셔플의 3분의 2가 원격으로 갔다. 496MB를
  없애는 이득은 로컬보다 클 것이다

둘 다 잰 값이 아니다. 클러스터에서 같은 실험을 돌리는 것은 이번 범위에 넣지 않았다.

## 결론

- **이 크기에서 로컬 시간은 세 방식이 같다.** 범위가 겹친다
- **broadcast는 셔플을 없애고 메모리를 산다.** 셔플 496MB 대신 저장 메모리 948MB
- **차원을 먼저 거르면 둘 다 줄어든다.** 셔플 94MB, 메모리 합 0.57GB로 세 방식 중 가장 가볍다.
  마트를 두 번 읽는 비용이 시간에 드러나지 않았다
- **운영 기본값은 그대로 broadcast다**(`with_names(broadcast=True)`). 로컬에서는 드라이버 8g 안에서
  돈다. 지금 설정의 클러스터(실행기 통합 메모리 456MB)로 옮기면 `pruned`로 바꾸거나 실행기 메모리를
  정해야 한다

## 측정하면서 고친 것

- **메모리가 0으로 나왔다.** Spark는 실행기 메모리를 기본으로 하트비트(10초)마다 잰다. 10초 남짓
  끝난 런은 한 번도 못 재서 0이 남았고, 그대로 두면 "브로드캐스트가 메모리를 안 썼다"로 읽힌다.
  측정 주기를 100ms로 줄이고, 한 번도 못 쟀으면 0이 아니라 "못 잼"을 내게 했다
- **계획이 최종 계획이 아니었다.** 쿼리에 `first()`를 부르면 내부에서 `limit(1)`을 붙인 새 쿼리가
  돈다. 계획을 뽑은 원래 쿼리는 실행되지 않아 아홉 런 모두 `isFinalPlan=false`였다. 이벤트 로그에서
  실제로 돈 계획을 꺼내게 바꾸고, 다시 돌리지 않고 저장된 로그로 다시 뽑았다(`--rederive`)
- **GC 시간이 런마다 쌓였다.** `TotalGCTime`은 JVM이 뜬 뒤부터의 누적값이다. 같은 JVM에서 아홉 런을
  돌리니 761, 1092 … 3670ms로 늘었다. 런 안에서 최대에서 최소를 뺀다

## 이 결과의 한계

- **로컬 모드다.** 위 "클러스터라면" 절 참고
- **크기 하나만 봤다.** 마트 1,479만 행, 차원 297만 버전. 차원이 커지면 broadcast가 드라이버
  메모리에서 먼저 막힌다
- **첫 런이 차갑다.** 세 방식의 순서를 회차마다 바꾸지 않았다. 중앙값이 첫 런을 걸러냈지만 설계로
  막은 것은 아니다

## 재현

```
./scripts/spark_run.sh -m 10g -- python scripts/experiments/experiment_broadcast.py --repeats 1 --out broadcast-precheck
./scripts/spark_run.sh -m 10g -- python scripts/experiments/experiment_broadcast.py --out broadcast-experiment
./scripts/spark_run.sh -- python scripts/experiments/experiment_broadcast.py --rederive --out broadcast-experiment
```

아홉 런에 약 5분. 결과는 데이터 루트의 `measurements/broadcast-experiment.json`, 런마다의 최종 계획은
`measurements/plans/broadcast-experiment/<방식>-<회차>.txt`, 이벤트 로그는 `eventlogs/broadcast-experiment/`.
