# MBID가 실제로 어디에 들어 있는가 (2026-09-07)

명세 6.3의 MBID 우선순위 규칙을 코드로 옮기기 전에, 그 규칙이 전제하는 필드가
실제 레코드에 있는지 확인한 결과다. 대상은 Bronze 155,713,649행 전수다.

## 레코드의 모양

최상위 키는 5개뿐이고 155,713,649행 전부가 같다.

```json
{
  "user_id": 3277,
  "user_name": "Izum",
  "timestamp": 1675677878,
  "recording_msid": "bdbbcdc8-6d56-4915-b10c-623d08a9c101",
  "track_metadata": {
    "track_name": "...",
    "artist_name": "...",
    "release_name": "...",
    "additional_info": { "submission_client": "...", "recording_msid": "..." }
  }
}
```

## MBID가 들어올 수 있는 자리

| 자리 | 행 | 비율 |
| --- | ---: | ---: |
| `track_metadata.mbid_mapping.recording_mbid` | 29,602 | 0.02% |
| `track_metadata.mbid_mapping.artist_mbids` | 29,568 | 0.02% |
| `additional_info.recording_mbid` | 4,157,182 | 2.67% |
| **`additional_info.lastfm_track_mbid`** | **51,144,858** | **32.85%** |
| `additional_info.artist_mbids` | 6,746,120 | 4.33% |
| **`additional_info.lastfm_artist_mbid`** | **57,328,649** | **36.82%** |
| `additional_info.release_mbid` | 4,544,552 | 2.92% |
| `additional_info.lastfm_release_mbid` | 47,060,471 | 30.22% |
| `recording_msid` | 155,713,649 | 100.00% |
| `submission_client` | 150,864,198 | 96.89% |

## 명세와 다른 점 셋

### 1. `mbid_mapping`이 사실상 없다 (0.02%)

명세는 최상위 `mbid_mapping`을 1순위로 본다. 최상위에는 **0행**이고,
`track_metadata` 아래에 29,602행(0.02%)이 있다.

`mbid_mapping`은 ListenBrainz가 서버에서 `recording_msid`를 MusicBrainz에
매칭해 만든 결과다. API 응답에는 붙어 나오지만 **덤프에는 사용자가 제출한
원본이 들어 있다.** 우리가 받는 것은 가공 전 데이터다.

msid → mbid 매핑 덤프가 `labs/mappings/`에 따로 있으나 마지막 판이
**2020년 6월**이다. 6년째 갱신이 없어 쓸 수 없다.

### 2. 클라이언트 MBID는 한 자리가 아니다

명세는 `additional_info.recording_mbid`만 본다. 그 자리는 2.67%인데
`lastfm_track_mbid`가 **32.85%**로 12배 많다.

Last.fm 임포터가 최대 유입 경로이기 때문이다(아래 클라이언트 분포 참고).
한 자리만 보면 실제로 있는 recording MBID의 **92%를 버린다.**

### 3. 클라이언트는 추론할 필요가 없다

명세 6.3의 2번은 "`additional_info` 키 조합 패턴으로 제출 경로 추론"이라고 한다.
그런데 `submission_client` 필드가 **96.89%**에 직접 들어 있다.

추론이 필요한 것은 나머지 3.11%뿐이고, 그마저도 근거가 약하면 `UNKNOWN`이 낫다.

| 제출 클라이언트 | 행 | 비율 |
| --- | ---: | ---: |
| ListenBrainz lastfm importer v2 | 79,858,017 | 51.29% |
| ListenBrainz Archive Importer | 43,774,219 | 28.11% |
| listenbrainz | 6,636,715 | 4.26% |
| navidrome | 5,103,970 | 3.28% |
| (없음) | 4,849,451 | 3.11% |
| Pano Scrobbler | 3,376,408 | 2.17% |
| Web Scrobbler | 2,067,736 | 1.33% |
| ListenBrainz Android | 1,910,904 | 1.23% |
| Roon | 599,137 | 0.38% |
| spotify-history-import | 538,078 | 0.35% |
| ListenBrainz plugin for Jellyfin | 421,224 | 0.27% |
| multi-scrobbler | 392,229 | 0.25% |

**임포터 두 개가 79.4%다.** 이 데이터는 실시간 청취 기록이라기보다
다른 서비스에서 가져온 이력이 대부분이다. 청취 시각이 21.5년에 걸쳐 있는 것과
같은 사실의 다른 면이다.

## 두 자리가 겹칠 때

`mbid_mapping.recording_mbid`와 `additional_info.recording_mbid`가
모두 있는 행은 **131개**뿐이고, 그 131개는 값이 전부 같다.

**표본이 너무 작아 두 출처의 불일치율은 말할 수 없다.**
"서버 해석과 클라이언트 제출이 어긋난다"는 가설은 이 데이터로 검증되지 않는다.
`mbid_source`를 나누는 근거는 불일치 관측이 아니라 **출처가 다르다는 사실 자체**다.

## 규칙 적용 결과

명세의 두 자리만 볼 때와, `lastfm_track_mbid`를 더할 때의 차이다.

| 규칙 | MBID 있음 | 없음 |
| --- | ---: | ---: |
| 명세대로 (`mbid_mapping` + `recording_mbid`) | 4,186,784 (2.69%) | 97.31% |
| `lastfm_track_mbid` 추가 | **55,331,502 (35.54%)** | 64.46% |

| `mbid_source` | 행 | 비율 |
| --- | ---: | ---: |
| MAPPING | 29,602 | 0.02% |
| CLIENT | 55,301,900 | 35.52% |
| NONE | 100,382,147 | 64.47% |

결정은 `docs/mbid_resolution.md`에 적었다.

## 재현

```
docker run --rm -m 8g -v <데이터 루트>:/data ... flux-spark \
  python scripts/inspect_mbid.py --root /data
```

결과는 데이터 루트의 `measurements/mbid-shape.json`에 있다.
