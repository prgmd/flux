# 녹음 연결 규칙 후보와 검수 사례

2026-10-19 기준. 아래는 검수한 연결 제안이며 Silver 수정이나 승인 매핑의 운영 적용이 아니다. 원본 MSID와 표기는 보존한다. 같은 녹음으로 연결한 뒤에도 서로 다른 시각의 반복 청취는 유지한다.

## 두 앨범 표기 검수

### GOLDEN과 GOLDEN [Explicit]

검수 대상은 Jung Kook의 Standing Next to You다. MSID `165376e5-fa83-4e04-8866-db39702f7cb2`와 `25cc1ee0-abc2-4a38-b525-6c2ce64fb968`의 모든 사전 변형을 확인했다. 비어 있지 않은 recording MBID는 둘 다 `4a89a693-a5a7-4348-9cf6-718aad1667fb` 하나이며 다른 녹음 ID는 없었다. 앞 MSID에는 대소문자 차이와 MBID 누락도 관측됐다.

보관한 상세 v3에서 두 MSID의 모든 기록을 다시 확인했다. 앞 MSID 30,735회는 앨범 GOLDEN, 뒤 MSID 1,000회는 GOLDEN [Explicit]였다. 다른 앨범 표기는 없었다. 연결만 하면 합계 31,735회를 보존하며 동일 시각 초과 제거는 별도 판단이다.

[Amazon의 곡 페이지](https://music.amazon.com/tracks/B0CLFNDT1Z)는 GOLDEN [Explicit] 앨범의 Standing Next to You를 3분 26초로 수록하며 곡 제목에 별도 Explicit 버전을 표시하지 않는다. 같은 앨범 안의 Seven은 Explicit Ver.와 Clean Ver.를 각각 표시한다. [MusicBrainz의 GOLDEN 수록 정보](https://musicbrainz.org/release/ab335506-0b6e-4730-bf4a-d3484b88ed54)도 함께 확인했다.

**판단: 이 두 MSID의 해당 곡은 같은 녹음으로 연결할 근거가 충분한 후보다.** 앨범에 붙은 Explicit 라벨과 곡 자체의 Explicit/Clean 버전을 구분한다. 이것은 오디오 파일의 일치 검증이 아니라 공식 수록 정보와 관측 메타데이터를 결합한 판단이다. GOLDEN의 모든 곡 또는 모든 앨범의 `[Explicit]`를 무조건 없애는 규칙으로 확장하지 않는다.

### Right Place, Wrong Person과 (Ver. A)

검수 대상은 RM의 LOST!다. MSID `fa1da800-a51f-494a-9f41-ce6ae8d6d461`과 `71efcac1-e969-4f98-a95f-01e51ed69702`의 모든 사전 변형에서 비어 있지 않은 MBID는 `c73941d9-7ac1-486d-bd88-3d3f8e944a7d` 하나였다. 두 MSID 모두 MBID가 없는 변형도 있지만 상충하는 MBID는 없었다.

상세 v3에서 앞 MSID 84,500회는 기본 앨범명, 뒤 MSID 123회는 (Ver. A) 앨범명뿐이었다. 연결만 할 때의 합계는 84,623회다. 이전 사례집의 84,473회와 118회는 MBID가 채워진 변형만의 수치이며, 여기에는 MBID 누락 기록도 포함했다.

[BTS 공식 스토어](https://shop.bts-official.uk/products/rm-right-place-wrong-person-version-a)는 이 앨범을 Ver. A, B, C 상품으로 판매하고 각 버전의 CD와 책자 및 구성품을 설명한다. [BIGHIT의 앨범 안내](https://bts.ibighit.com/eng/discography/rm/detail/rpwp/)의 LOST! 수록 정보와 함께 검수했다.

**판단: 이 두 MSID의 LOST!는 같은 녹음으로 연결하는 것이 타당한 후보다.** 이 앨범의 상품 버전 표기라는 외부 근거와 동일 녹음 MBID가 함께 있다. `Ver.`가 항상 포장 차이라는 뜻은 아니다. 다른 앨범, 언어 버전과 재녹음 버전에 그대로 적용하지 않는다. MBID가 전혀 없는 다른 MSID는 이 두 쌍의 결론만으로 자동 승인하지 않는다.

## 규칙 후보

| 규칙 | 후보를 만드는 조건 | 연결 판단 |
| --- | --- | --- |
| R1 앨범 내용 라벨 | 같은 아티스트와 곡, 앨범의 `[Explicit]` 차이 | 공식 수록 정보와 녹음 ID 또는 다른 독립 근거 일치, 곡별 clean/explicit 차이 없음 확인 후 한정 승인 |
| R2 상품 버전 | 같은 아티스트와 곡, `(Ver. A)` 같은 앨범 차이 | 해당 앨범의 공식 상품 구성과 녹음 정보가 뒷받침할 때만 한정 승인 |
| R3 속도와 효과 | sped up, slowed, nightcore, reverb, pitch | 같은 녹음에 자동 합치지 않는다. 필요하면 작품 묶음 아래 별도 버전으로 연결 |
| R4 편곡과 공연 | live, remix, acoustic, instrumental, drumless | 별도 녹음 후보로 유지하고 공식 버전 확인 |
| R5 리마스터와 편집 | remastered, radio edit, extended, mono, stereo | 일괄 합치지 않고 프로젝트가 구분할 녹음 단위를 먼저 결정 |
| R6 피처링과 언어 | feat., with, 언어명과 번역 제목 | 크레딧 또는 언어 버전의 차이를 확인. 괄호 문구만 삭제하지 않음 |
| R7 미분류 | 알려진 규칙에 없는 괄호나 접미사 | 원래 제목의 일부일 수 있어 원문 보존, 수동 검수 대기 |

검수 기록에는 원본 MSID, 제안된 녹음 묶음 키, 규칙 ID와 버전, 공식 자료, 상충 증거, 판단 상태와 검수 시점을 남긴다. 규칙의 적용 범위는 아티스트와 곡 및 앨범으로 한정할 수 있어야 한다. 승인과 보류 및 분리 유지 상태를 구분하며 이름이 같다는 이유만으로 자동 승인하지 않는다.

## 전수 후보 사례집의 범위

`scripts/investigations/catalog_title_variants.py`는 월별 Silver의 메타데이터 사전 전체를 읽는다. 마지막 괄호, 대괄호 또는 공백으로 구분한 하이픈 뒤 문구를 수집하고 유형별 후보 목록을 만든다. 마지막 문구 하나만 추출하며 여러 접미사를 재귀적으로 모두 제거하지 않는다. 예를 들어 `Song (Live) [Remastered]`의 후보 본체는 `song (live)`다. 이 절제는 라이브 정보를 지워 원곡과 합치는 것을 피하기 위한 조사 범위다.

동일 아티스트와 후보 본체 안에 서로 다른 정규화 제목이 있는 경우를 저장한다. 원래 제목만 있고 변형이 없으면 연결 후보가 아니며, 원래 제목이 없어도 서로 다른 변형이 두 개 있으면 후보가 될 수 있다. 알려진 유형에 맞지 않는 접미사도 미분류 목록에 남긴다. 별도로 같은 아티스트와 MBID에 서로 다른 제목이 붙은 사례를 전부 수집한다. 오연결된 MBID도 있으므로 이 목록 역시 정답이 아니다.

**전체 사전을 스캔했다는 것과 모든 동일 곡 관계를 발견했다는 것은 다르다.** 제목 오타와 번역, 아티스트 별칭, 구분자 없는 속도 표기, 중첩 괄호와 현재 규칙에 없는 언어는 빠질 수 있다. MBID가 없고 제목도 크게 다르면 추가 연결 근거가 필요하다. 이번 목록을 사례집의 첫 버전으로 삼고 미분류와 누락 사례를 보고 규칙을 보강한다.

대량 사례는 무작위로 모두 읽기보다 고유 청취자 수와 영향받는 청취 수를 붙여 우선순위를 정한다. AI가 유형과 근거를 제안하고 사람이 유형별 오탐과 경계 사례를 검수한다. 그 뒤 승인된 좁은 규칙만 적용하고 적용 전후 차이를 대조한다. 이번 조사에서 후보 목록의 크기를 삭제 가능한 곡 수로 해석하지 않는다.

## 전수 스캔 결과와 사례집

사전 v0의 23,965,606행을 232.15초에 스캔했다. 완료 상태와 입력 로그 버전 및 해시 불변을 확인했다. 청취 상세 전량의 사건 중복을 다시 계산한 시간은 아니다.

| 산출물 | 행 수 | 의미 |
| --- | ---: | --- |
| qualifier_inventory.parquet | 1,032,949 | 관측된 마지막 접미사와 대표 분류의 목록 |
| title_candidates.parquet | 760,451 | 같은 아티스트와 후보 본체에 제목 변형이 여러 개인 묶음 |
| title_members.parquet | 6,810,275 | 위 묶음의 원본 제목과 MSID 및 MBID 조합 |
| mbid_candidates.parquet | 83,222 | 같은 아티스트와 MBID에 서로 다른 제목이 있는 묶음 |
| mbid_members.parquet | 477,026 | MBID 경로 후보의 원본 조합 |

접미사 목록에서 미분류는 576,878종이다. 숫자와 장소, 공연 날짜, 제목의 본래 부제도 포함하므로 이 수를 오류 개수로 볼 수 없다. 분류는 속도/효과, 라이브, 리믹스 등의 순서로 처음 일치한 대표 유형 하나를 부여한다. 복합 문구 원문은 남기며 대표 유형만으로 버전의 모든 특성을 확정하지 않는다.

후보 묶음 중 속도나 효과 유형이 포함된 묶음은 17,614개다. 리믹스 173,947개, 라이브 99,746개, 리마스터 54,764개, 내용 라벨 6,698개도 관측됐다. 하나의 묶음이 여러 유형을 포함할 수 있어 이 숫자를 더하지 않는다. MBID 경로와 접미사 경로의 후보도 서로 겹칠 수 있다.

실제 원문을 확인한 예시는 다음과 같다. 이 표는 공식 음원의 존재를 확정하는 표가 아니라 수집된 제목 변형의 사례다.

| 관측 아티스트 | 기본 표기와 실제 변형 | 우선 판단 |
| --- | --- | --- |
| Lady Gaga | Just Dance / Just Dance (Speed Up) / Just Dance (Sped Up) | 속도 변경은 별도 버전으로 보존. 두 속도 표기가 서로 같은 편집인지도 별도 검수 |
| The Weeknd | Starboy / Starboy (Speed Up + Reverb) / Starboy (slowed + reverb) | 원곡과 효과 버전을 분리 |
| Arctic Monkeys | 505 / 505 (slowed + reverb + bass boosted) | 속도와 잔향 및 저음 효과 차이 보존 |
| Radiohead | Creep / creep [slowed HQ Audio] / Creep [Clean] / Creep [Explicit] | 속도와 내용 버전을 별도로 검수 |
| Radiohead | No Surprises / No Surprises (Remastered) / No Surprises (Lyrics) | 리마스터와 영상 부가 표기를 같은 규칙으로 지우지 않음 |
| Taylor Swift | Blank Space / Blank Space (Taylor's Version) / Blank Space (Voice Memo) | 버전과 메모를 표기 차이로 일괄 합치지 않음 |

재현 명령은 다음과 같다. 출력은 원본과 겹치지 않는 새 디렉터리여야 한다. 큰 사례집과 DuckDB는 로컬 조사 산출물로 보관하고 git에는 로직과 검수 문서만 저장한다.

```powershell
$env:PYTHONPATH='src'
venv/Scripts/python scripts/investigations/catalog_title_variants.py --source <월별Silver>/recording_metadata --output <새사례집폴더>
```

`result.json`의 completed와 source_unchanged를 확인한다. 목록의 모든 행을 사람이 검수한 것은 아니다. 우선 검수한 두 앨범 사례와 유형별 대표 사례를 바탕으로 검수 상태를 쌓아 갈 준비가 된 상태다. 앨범명 전체의 변형 목록은 월별 사전에 앨범명이 없으므로 이번 제목 스캔 범위에 포함되지 않는다.
`tests/test_title_variant_catalog.py`와 기존 조사 테스트 합계 8개가 통과했다. 내보낸 다섯 Parquet의 행 수를 결과 파일과 대조했고, 두 후보 경로 모두 구성원에서 다시 센 제목 수가 후보 표와 전수 일치했다. 이 검사는 사례집의 계산 및 저장 무결성을 확인하며 실제 곡 연결의 정확성을 대신하지 않는다.


## R8 공식 MBID 안의 문자 차이 정리

승인한 규칙 버전은 `official-credit-typography-v1`이다. 공식 recording MBID가 실제 덤프에서 조회된 경우에 한해, 비교용 제목과 아티스트에서 U+2018/U+2019 따옴표를 ASCII 작은따옴표로, U+2010/U+2011 하이픈을 ASCII 하이픈으로 정리한다. 대소문자와 앞뒤 공백 처리는 기존과 같다. 원문, MSID와 MBID는 바꾸지 않는다.

이 규칙은 다른 MBID의 이름을 비교해 합치는 규칙이 아니다. MBID 충돌, 미수록, 버전 설명과 비디오는 기존 검토 조건을 유지한다. 괄호와 clean, explicit, sped up 표기를 지우지 않는다. 공식 이름과 비교가 통과해도 음원 동일성을 완전히 입증하는 것은 아니므로 제안 크레딧과 최종 적용 상태를 구분한다. 동명이인, 번역 제목, 관사와 별칭은 이 규칙 범위 밖이다.

`tests/test_official_credit_audit.py`에서 문자 차이 통과와 미수록 ID 보류, 속도 변경 제목 보류, censored 및 live 설명 유지와 원문 보존을 확인한다. 첫 실패 후 구현했으며 전체 조사용 테스트 5개가 통과했다. 전수 적용 영향은 [전체 점검 결과](review_20261019.md)에 기록한다.
