# 중복끼리 무엇이 다른가 (2026-09-14)

중복 키의 17.9%가 내용이 서로 다르다(`2026-09-07-duplicates.md`).
어느 쪽을 남길지 정하려면 무엇이 다른지를 봐야 한다.

내용이 다른 키 400개(854행)를 뽑아, 덤프 순서로 정렬한 뒤 **가장 이른 것과
가장 늦은 것**을 비교했다.

## 나중 것이 더 낫지 않다

| 나중 덤프의 필드 수 | 키 | 비율 |
| --- | ---: | ---: |
| 더 많다 | 7 | 1.8% |
| **더 적다** | **35** | **8.8%** |
| 같다 | 358 | 89.5% |

**"나중 것을 남긴다"는 규칙은 8.8%에서 정보를 잃는다.**

사라진 필드를 보면 손해가 분명하다.

| 나중에 사라진 필드 | 키 |
| --- | ---: |
| `additional_info.lastfm_artist_mbid` | 34 |
| `additional_info.lastfm_track_mbid` | 5 |
| `additional_info.submission_client` | 3 |
| `additional_info.lastfm_release_mbid` | 2 |
| `additional_info.artist_mbids` | 1 |

**사라지는 것의 대부분이 MBID다.** MBID는 dimension 연결의 유일한 수단이고
이미 64%가 없는 상태(`2026-09-07-mbid-shape.md`)라, 있는 것을 버리면 안 된다.

나중에 생긴 필드도 있지만 훨씬 적다(`recording_mbid` 4, `lastfm_track_mbid` 3,
`listening_from` 3, `artist_mbids` 2, 나머지 1건씩).

## 값이 바뀐 필드

| 필드 | 키 | 비율 |
| --- | ---: | ---: |
| **`user_id`** | **294** | **73.5%** |
| `additional_info.submission_client` | 83 | 20.8% |
| `additional_info.lastfm_track_mbid` | 13 | 3.2% |
| `additional_info.lastfm_artist_mbid` | 1 | 0.2% |

### `lastfm_track_mbid`가 바뀐다

같은 청취인데 MBID 값 자체가 달라진 경우다.

```
앞 (08-23) "lastfm_track_mbid":"e79bde2e-9129-4daf-aa81-75b0507a1caf"
뒤 (09-03) "lastfm_track_mbid":"744f3f4c-94ed-4375-a853-7aca95ace9de"
```

Last.fm이 같은 곡을 나중에 **다른 MusicBrainz 항목으로 매칭**했다는 뜻이다.
어느 쪽이 맞는지 우리 데이터로는 판단할 수 없다. MusicBrainz 쪽에서
항목이 병합되거나 분리되면 이런 일이 생긴다.

`mbid_source = LASTFM`을 따로 둔 판단이 여기서 값을 한다. 이 흔들림이
`CLIENT`와 섞여 있었으면 원인을 짚을 수 없었다.

### `submission_client`가 바뀐다

20.8%에서 제출 클라이언트가 다르다. 같은 청취를 **다른 경로로 다시 제출**한
것이므로 자연스럽다. 임포터가 79.4%인 데이터에서 예상되는 결과다.

### `user_id`가 73.5%에서 바뀐다 — 표본이 틀렸다

> [!warning] 이 수치는 잘못됐다
> 전수로 확인하니 번호가 갈린 이름은 38,854명 중 **7명(0.02%)**뿐이었다.
> 73.5%는 표본 추출 방식의 편향이 만든 값이다.
>
> `groupBy` 뒤에 `limit(400)`을 쓴 것이 원인이다. `limit()`은 표본이 아니라
> **먼저 끝난 셔플 파티션에서 앞의 N개를 집어온다.** 셔플 파티션은 키 해시로
> 나뉘므로 표본이 몇 명의 사용자에 몰리고, 그중 하나가 번호가 갈린 7명에
> 속하면 표본 대부분이 그렇게 보인다.
>
> 전수 결과와 원인은 `2026-09-14-user-identity.md`에 있다.
> 스크립트는 `orderBy(F.rand(seed))`로 섞은 뒤 자르도록 고쳤고,
> 표본에 사용자가 몇 명 들어왔는지도 함께 출력한다.

**아래 수치들도 같은 편향을 안고 있다.** 무작위 표본으로 다시 잰 결과가
이 문서 끝의 「다시 잰 결과」다. 위 표는 편향된 첫 측정으로 남겨 둔다.

---

# 다시 잰 결과 (무작위 표본 2,000키)

`orderBy(F.rand(20260914))`로 섞은 뒤 2,000개를 뽑았다. 4,383행이 걸렸다.

## 표본에 사용자가 23명뿐이다

**이것 자체가 발견이다.** 무작위로 뽑았는데도 2,000개 키가 **23명**에서 나왔다
(사용자당 87개).

내용이 다른 중복은 **소수 사용자에 몰려 있다.** 한 사람이 Last.fm 이력 전체를
다시 가져오면 그 순간 수만 건의 중복이 한꺼번에 생기기 때문이다.

> 아래 비율은 **그 23명의 데이터를 설명하는 값**이다. 전 사용자로 일반화되지 않는다.
> 내용이 다른 중복을 가진 사용자가 전체 몇 명인지는 재지 않았다.

## 나중 것이 더 낫지 않다 — 편향을 걷어내니 더 뚜렷하다

| 나중 덤프의 필드 수 | 키 | 비율 |
| --- | ---: | ---: |
| 더 많다 | 49 | 2.5% |
| **더 적다** | **258** | **12.9%** |
| 같다 | 1,693 | 84.7% |

첫 측정(8.8%)보다 **더 나쁘다.** 나중 것을 남기면 12.9%에서 필드를 잃는다.

| 나중에 사라진 필드 | 키 |
| --- | ---: |
| `additional_info.lastfm_artist_mbid` | 262 |
| `additional_info.lastfm_track_mbid` | 29 |
| `additional_info.lastfm_release_mbid` | 18 |
| `additional_info.submission_client` | 12 |
| `additional_info.artist_mbids` | 4 |
| `additional_info.spotify_id`, `isrc`, `artist_names` 등 | 3씩 |

| 나중에 생긴 필드 | 키 |
| --- | ---: |
| `additional_info.recording_mbid` | 29 |
| `additional_info.artist_mbids` | 20 |
| `additional_info.lastfm_track_mbid` | 16 |
| `additional_info.listening_from` | 12 |
| `release_mbid`, `youtube_*`, `music_service`, `origin_url` | 10씩 |

**양쪽 모두 MBID가 오간다.** 나중 것이 `recording_mbid`를 얻기도 하고
`lastfm_artist_mbid`를 잃기도 한다. **어느 한쪽을 통째로 고르면 반드시 손해다.**

## 값이 바뀐 필드

| 필드 | 키 | 비율 |
| --- | ---: | ---: |
| `user_id` | 1,407 | 70.3% |
| `additional_info.submission_client` | 459 | 22.9% |
| `additional_info.lastfm_track_mbid` | 69 | 3.5% |
| `additional_info.lastfm_release_mbid` | 2 | 0.1% |
| `track_metadata.artist_name` | 2 | 0.1% |
| `additional_info.ms_played` | 1 | 0.1% |

### `user_id` 70.3%는 전수와 모순되지 않는다

전수로는 번호가 갈린 이름이 7명(0.02%)뿐인데 여기서는 70.3%다.
**표본의 사용자가 23명이기 때문**이다. 그 23명 중 일부가 번호 갈린 7명에 속하면
이 비율이 나온다.

두 수치가 함께 말하는 것은 이렇다.
**번호가 갈린 소수의 사용자가 내용이 다른 중복의 대부분을 만든다.**
계정 번호가 바뀐 사람이 이력을 다시 가져왔다고 보면 자연스럽다.

`user_id`를 `dim_user` FK로 쓰는 설계에는 영향이 없다
(`2026-09-14-user-identity.md`).

### `submission_client`가 22.9%에서 바뀐다

같은 청취를 **다른 경로로 다시 제출**한 것이다. 임포터가 79.4%인 데이터에서
예상되는 결과다.

## 재현

```
docker run --rm -m 12g -v <데이터 루트>:/data \
  -e FLUX_STORAGE_ROOT=file:///data ... flux-spark \
  python scripts/investigations/inspect_duplicate_diff.py --keys 400
```

결과는 데이터 루트의 `measurements/duplicate-diff.json`에 있다.

## 이 결과가 정하는 것

tie-break 규칙은 **시간순이 아니라 정보량 기준**이어야 한다.
"나중 것"은 8.8%에서 MBID를 버리고, "앞의 것"은 1.8%에서 버린다.
어느 한쪽을 통째로 고르는 규칙은 둘 다 손해다.

결정은 `docs/deduplication.md`에 적는다.
