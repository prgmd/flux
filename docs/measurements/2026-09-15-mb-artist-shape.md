# MusicBrainz artist 테이블의 모양 (2026-09-15)

`dim_artist`를 만들기 전에 원본 테이블의 실제 모양을 확인한 결과다.
릴리스 `20260905-002519`, `mbdump.tar.bz2` 안의 테이블들이다.

## 덤프 파일에는 헤더가 없다

MusicBrainz 덤프의 테이블 파일은 PostgreSQL COPY 형식이다. 탭으로 구분되고
**첫 줄이 데이터**다. 컬럼 이름이 파일 어디에도 없다.

그래서 컬럼 순서는 **MusicBrainz 스키마 정의**에서 와야 한다. 순서를 틀리면
이름 자리에 나라 번호가 들어가는데 **프로그램은 그대로 돌고 결과만 조용히 틀린다.**

값의 모양으로 검산했다.

| 검산 | 결과 |
| --- | --- |
| `id`가 숫자 | 통과 |
| `gid`가 UUID 길이(36)이고 하이픈 포함 | 통과 |
| `name`이 비지 않음 | 통과 |
| `ended`가 `t` 또는 `f` | 통과 |
| `edits_pending`이 숫자 | 통과 |

표본 30행 전부에서 컬럼이 **19개로 일정**했고, 기대한 순서와 맞았다.

## `artist` — 2,976,554행, 19컬럼

```
 0 id                 2252039
 1 gid                fadeb38c-833f-40bc-9d8c-a6383b38b1be
 2 name               Pete Moutso
 3 sort_name          Moutso, Pete
 4 begin_date_year    \N
 5 begin_date_month   \N
 6 begin_date_day     \N
 7 end_date_year      \N
 8 end_date_month     \N
 9 end_date_day       \N
10 type               1
11 area               \N
12 gender             \N
13 comment            (빈 문자열)
14 edits_pending      0
15 last_updated       2021-11-23 07:08:52.479537+00
16 ended              f
17 begin_area         \N
18 end_area           \N
```

### 빈 값이 많다 (표본 30행)

| 컬럼 | 빈 행 |
| --- | ---: |
| `end_date_*` | 30 / 30 |
| `begin_date_month`, `begin_date_day` | 30 / 30 |
| `begin_area`, `end_area` | 29~30 / 30 |
| `begin_date_year` | 27 / 30 |
| `area` | 26 / 30 |
| `gender` | 25 / 30 |
| `type` | 17 / 30 |
| `last_updated` | 12 / 30 |

**명세의 `dim_artist` 컬럼 대부분이 비어 있다.** `country`, `type`,
`begin_date`, `end_date` 넷이 그렇다. 표본이 30행이라 전수는 아니지만
방향은 분명하다.

### 없음의 표현이 둘이다

`\N`이 NULL이고 빈 문자열은 값이다. `comment`는 빈 문자열로 들어온다.

읽는 쪽에서 둘을 같게 볼지 다르게 볼지 정해야 한다.

## `type`, `area`, `gender`는 숫자 외래 키다

**명세가 놓친 부분이다.** 5.3의 `dim_artist`는 `country`와 `type`을 값으로
갖는데, 원본에는 번호만 있다. 조회 테이블을 함께 실어야 한다.

| 테이블 | 행 수 | 쓰임 |
| --- | ---: | --- |
| `area` | 120,071 | `artist.area`가 가리키는 지역 |
| `artist_type` | **6** | `artist.type`이 가리키는 종류 |
| `gender` | 5 | `artist.gender`가 가리키는 값 |
| `iso_3166_1` | 258 | `area` → 국가 코드 |

`artist_type`은 여섯 개뿐이다.

```
1 Person   2 Group   3 Other   4 Character   5 Orchestra   6 Choir
```

## `area`는 국가가 아니다

가장 조심할 부분이다. `area` 표본이다.

```
id=38,    name=Canada,  type=1
id=43,    name=Chile,   type=1
id=15449, name=Greccio, type=4
```

`Greccio`는 이탈리아의 소도시다. **`area`는 나라, 주, 도시를 모두 담는 표**이고
`type`이 그 층위를 가른다.

`artist.area`를 그대로 `country`에 넣으면 **도시가 국가 자리에 들어간다.**

국가만 골라내려면 `iso_3166_1`을 거쳐야 한다. 이 표는 `area`의 id를
ISO 국가 코드로 이어 주므로, **여기에 없는 `area`는 국가가 아니다.**

```
area=1 → AF      area=2 → AL      area=3 → DZ
```

258행이라 실제 국가 수와 맞는다.

> 그러면 도시 소속 아티스트의 `country`는 어떻게 되는가. `area`의 계층을
> 거슬러 올라가 상위 국가를 찾는 방법이 있으나, 그건 `area_containment`
> 같은 별도 표가 필요하다. **`iso_3166_1`에 직접 걸리는 경우만 채우고
> 나머지는 비우는 것이 정직하다.** 얼마나 비는지는 적재 후 잰다.

## 이 결과가 정하는 것

`dim_artist` 하나를 만들려면 **다섯 표를 실어야 한다.**

```
artist  +  artist_type  +  area  +  iso_3166_1  ( + gender )
```

명세 6절의 잡 목록에는 `load_musicbrainz` 하나로 적혀 있는데,
**조회 표를 함께 싣는 것까지가 그 잡의 범위**다.

규모는 부담되지 않는다. `artist` 297만 행이 가장 크고 나머지는 12만, 258, 6, 5행이다.
`recording` 4,007만 행에 비하면 작다.

## 재현

```
FLUX_STORAGE_ROOT=<데이터 루트> python scripts/investigations/inspect_mb_artist.py --rows 30
FLUX_STORAGE_ROOT=<데이터 루트> python scripts/investigations/inspect_mb_lookups.py
```

결과는 데이터 루트의 `measurements/mb-artist-shape.json`과
`measurements/mb-lookup-shape.json`에 있다.
