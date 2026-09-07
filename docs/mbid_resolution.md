# MBID 우선순위 해석 규칙

명세 6.3의 규칙을 실측(`measurements/2026-09-07-mbid-shape.md`)에 맞춰 고친 것이다.

## 명세의 규칙

```
IF mbid_mapping 존재 AND recording_mbid가 유효한 UUID   → MAPPING
ELSE IF additional_info.recording_mbid 존재 AND 유효     → CLIENT
ELSE                                                     → NULL, NONE
```

이 규칙을 그대로 적용하면 MBID를 가진 행이 **2.69%**에 그친다.
실제로는 35.54%가 MBID를 갖고 있고, 차이는 전부 `lastfm_track_mbid`에서 온다.

## 고친 규칙

```
IF track_metadata.mbid_mapping.recording_mbid 가 유효한 UUID   → MAPPING
ELSE IF additional_info.recording_mbid 가 유효한 UUID          → CLIENT
ELSE IF additional_info.lastfm_track_mbid 가 유효한 UUID       → LASTFM
ELSE                                                            → NULL, NONE
```

아티스트도 같은 모양으로 푼다.

```
mbid_mapping.artist_mbids  →  additional_info.artist_mbids  →  lastfm_artist_mbid
```

### 왜 `mbid_mapping`을 `track_metadata` 아래에서 찾는가

명세는 최상위에 있다고 본다. 최상위에는 **0행**이고 `track_metadata` 아래에만 있다.

### 왜 `LASTFM`을 값으로 더하는가

`recording_mbid`(2.67%)와 `lastfm_track_mbid`(32.85%)는 **출처가 다르다.**
앞은 스크로블러가 직접 넣은 값이고, 뒤는 Last.fm 임포터가 Last.fm의
데이터베이스에서 가져온 값이다.

둘을 `CLIENT` 하나로 묶으면 그 값의 93%가 Last.fm에서 온 것이 되고,
**둘 중 하나가 변해도 분포에 안 나타난다.** 명세가 이 컬럼에 요구한 것이
"비율 급변이 곧 이상 신호"이므로, 묶으면 그 기능을 잃는다.

**이 값은 신뢰도 순위가 아니라 출처 표시다.** `LASTFM`이 `CLIENT`보다
덜 믿을 만하다고 주장하는 것이 아니다. 실측에서 두 출처가 겹치는 행은
9행 이하라 **불일치율을 잴 수 없었고**, 따라서 순위를 매길 근거가 없다.
컬럼 이름이 `mbid_source`인 만큼 출처를 적는 것으로 충분하다.

### 우선순위가 거의 작동하지 않는다

세 자리는 사실상 서로 겹치지 않는다.

- `mbid_mapping` ∩ `recording_mbid` = 131행
- `recording_mbid` ∩ `lastfm_track_mbid` ≤ 9행

(뒤엣것은 직접 세지 않고 집계에서 유도했다. 두 자리 행 수의 합이
55,302,040이고 우선순위 적용 후 `CLIENT`가 55,301,900이므로 차이가 140행이며,
그중 131행이 `mbid_mapping`과의 겹침이다.)

그러므로 **우선순위는 지금 데이터에서 결과를 거의 바꾸지 않는다.**
그래도 규칙으로 못 박아 두는 이유는 겹칠 때 무엇을 고를지가
사람마다 달라지면 안 되기 때문이다.

## 클라이언트 식별

명세 6.3의 2번은 `additional_info` 키 조합으로 제출 경로를 추론하라고 한다.
**추론하지 않는다.** `submission_client` 필드가 96.89%에 직접 들어 있다.

```
IF additional_info.submission_client 존재  → 그 값
ELSE                                        → 'UNKNOWN'
```

키 조합 추론은 나머지 3.11%를 위한 것인데, 근거가 약한 추측으로 채우는 것보다
`UNKNOWN`으로 두는 편이 낫다. **모르는 것을 모른다고 적어야 나중에 셀 수 있다.**

## UUID 검증

세 자리 모두 값이 UUID 형식인지 확인한다. 형식이 아니면 그 자리는 없는 것으로
보고 다음 자리로 넘어간다.

명세 6.3의 품질 규칙은 "MBID가 UUID 형식이 아니면 해당 필드만 NULL,
레코드는 통과"라고 한다. 그 원칙을 우선순위 안으로 넣은 것이다.

## 예상 분포

| `mbid_source` | 행 | 비율 |
| --- | ---: | ---: |
| MAPPING | 29,602 | 0.02% |
| CLIENT | 4,157,051 | 2.67% |
| LASTFM | 51,144,849 | 32.85% |
| NONE | 100,382,147 | 64.47% |

`CLIENT`와 `LASTFM`은 위 겹침 계산에서 나온 근사치다.
구현 후 실제 값으로 이 표를 갱신한다.

## 남는 문제

**64.47%가 MBID 없이 남는다.** 이 행들은 `recording_msid`만 갖고 있어
MusicBrainz dimension에 직접 붙지 않는다.

- ListenBrainz의 msid → mbid 매핑 덤프는 2020년판이 마지막이라 쓸 수 없다
- 아티스트 이름으로 맞추는 방법이 있으나 그 자체가 별도의 문제다

당장은 **붙지 않는다는 사실을 기록한다.** `mbid_source = NONE`의 비율이
곧 이 프로젝트의 dimension 연결률 한계이고, 그것을 숨기지 않는 것이
`mbid_source` 컬럼을 두는 이유다.
