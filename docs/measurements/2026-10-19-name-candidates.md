# 이름이 같은 녹음 후보 조사

## 범위와 실행 상태

2026-10-19 월별 Silver의 메타데이터 사전과 보관한 상세 검증본을 읽기 전용으로 조사했다. 이름 비교는 아티스트명과 곡명에 소문자와 앞뒤 공백 정리만 적용한다. 내부 공백, 피처링 표기, Explicit와 라이브 같은 문구는 제거하지 않는다. 아래는 전체 후보의 대표성 있는 무작위 표본이 아니라 기존에 논의한 곡의 목적 표본이다.

첫 전체 실행은 6GB 메모리 제한에서 실패했다. 숫자형 작업 키와 16GB 제한을 적용한 두 번째 실행은 제한된 프로세스 조회에서 보이지 않아 종료됐다고 잘못 판단했다. 기존 실행 세션과 호스트 프로세스로 확인하니 계속 계산 중이었다. 결과 파일 부재는 종료 증거가 아니었다. 동일 작업을 새로 실행하지 않고 기존 세션을 이어서 확인했다.

두 번째 실행은 최종 `completed=true`, `source_unchanged=true`로 완료했다. 이름 단계는 1,400.24초, MBID 단계를 포함한 전체 계산은 1,691.35초였다. 입력은 월별 표 v0와 사전 v0, 상세 v3이며 전후 Delta 로그 버전과 해시가 같았다. 처음 실행을 포함한 총 대기 시간과는 다르다.

### 이름 기준 전수 가정 결과

| 항목 | 결과 |
| --- | ---: |
| 여러 MSID를 가진 이름 후보 묶음 | 3,589,911개 |
| 후보 MSID | 13,359,504개 |
| 연결만 적용한 월별 행 수 | 75,460,461행 |
| 연결만 적용한 전체 횟수 | 157,351,363회 |
| 같은 사용자와 시각의 중복 후보 사건 묶음 | 3,280,002개 |
| 사건당 하나로 볼 때 초과 횟수 | 3,299,030회 |
| 해당 가정 적용 후 전체 횟수 | 154,052,333회 |
| 영향을 받는 사용자+이름 후보+월 | 1,446,168개 |
| 후보 월별 횟수와 상세의 불일치 | 0개 |

이름 연결만으로 월별 행은 5,103,524행 줄지만 총 횟수는 그대로다. 동일 사건 가정까지 적용하면 약 2.10%가 줄어든다. 아래 버전 충돌 때문에 이를 전체 데이터의 확정 중복률이나 삭제 대상 329만 건으로 발표하지 않는다. MBID 가정의 214,961회와는 대상이 겹치므로 더하지 않는다.

## 메타데이터 사전에서 찾은 후보

아래 숫자는 이름 쌍에 일치하는 관측 메타데이터에서 센 서로 다른 ID 수다. MSID의 다른 변형까지 이름이 일관된지 검사한 전체 조사 매핑의 크기와는 다르다. MBID가 다르다고 반드시 다른 녹음인 것도 아니며 같은 MBID가 붙었다고 연결이 옳다고 보장하지도 않는다.

| 관측 아티스트와 곡명 | 서로 다른 MSID | 비어 있지 않은 MBID |
| --- | ---: | ---: |
| Radiohead - Creep | 363 | 18 |
| Radiohead - Pyramid Song | 121 | 4 |
| Radiohead - Reckoner | 80 | 6 |
| Radiohead - Knives Out | 78 | 4 |
| Taylor Swift - Bad Blood | 78 | 3 |
| Daft Punk - Get Lucky | 76 | 8 |
| Jung Kook - Standing Next to You | 46 | 2 |
| RM - LOST! | 28 | 1 |
| Jungkook - Standing Next to You | 5 | 1 |

`Jung Kook`와 `Jungkook`는 현재 이름 규칙에서 별도 그룹이다. 아티스트 별칭을 확인한 매핑이 필요하며 모든 내부 공백을 지우는 방식으로 확장하지 않는다.

## 공식 녹음 정보로 구분한 실제 사례

| 후보 | 실제로 관측된 연결 | 판단 |
| --- | --- | --- |
| Reckoner | [기본 녹음](https://musicbrainz.org/ws/2/recording/d9b46ecb-5472-4dcd-8fa4-dd6723189e27?inc=artist-credits&fmt=json)과 [From the Basement 라이브](https://musicbrainz.org/ws/2/recording/694065b4-3910-49c8-a4dd-cb5d270201a1?inc=artist-credits&fmt=json)가 같은 로컬 이름에 포함됨 | 같은 제목이지만 녹음 버전은 구분한다. 라이브 표지가 로컬 제목에 없을 수 있다. |
| Bad Blood | [Taylor Swift 단독](https://musicbrainz.org/ws/2/recording/914f6a27-fce4-4a75-9267-049b51a1b086?inc=artist-credits&fmt=json), [Kendrick Lamar 피처링](https://musicbrainz.org/ws/2/recording/d112254a-b6a1-4993-bdd9-ea4cd0926d4b?inc=artist-credits&fmt=json), [2024년 라이브](https://musicbrainz.org/ws/2/recording/7fad1aa3-d874-48d8-842a-9ca9afea068e?inc=artist-credits&fmt=json)가 모두 로컬 Taylor Swift + Bad Blood에 연결됨 | Kendrick을 곡 제목 전체에 일괄 부여하거나 제외하지 않는다. 녹음별 크레딧으로 귀속해야 한다. |
| Get Lucky | [기본 녹음](https://musicbrainz.org/ws/2/recording/833f00e1-781f-4edd-90e4-e52712618862?inc=artist-credits&fmt=json)과 [Drumless Edition](https://musicbrainz.org/ws/2/recording/dbeffc1d-8f04-483e-8d02-270ad4922b28?inc=artist-credits&fmt=json)이 같은 로컬 제목에 포함됨 | 버전 표지가 누락된 사례다. 제목만 보고 합치지 않는다. |
| Get Lucky | 로컬 제목과 달리 [공식 제목이 Episode 4, Pharrell Williams인 ID](https://musicbrainz.org/ws/2/recording/a5972f29-04ab-4287-a68a-88a53478a512?inc=artist-credits&fmt=json)가 관측됨 | 메타데이터 연결 충돌 후보로 분리한다. 원본 오표기와 MBID 오연결 중 어느 쪽인지 추가 검수한다. |

Get Lucky에는 공식 길이가 369,000ms, 237,000ms, 40,013ms인 서로 다른 ID도 있다. 길이는 녹음 메타데이터이고 실제 재생 시간이 아니다. 길이만으로 편집 버전이나 미리듣기라고 확정하지 않는다. 일부 ID는 공식 API에서 404를 반환했으며 조회 실패를 동일 곡이나 무효 청취의 증거로 쓰지 않는다. 일시적 503 두 건은 재조회에 성공했다.

같은 작품의 여러 녹음을 하나의 분석용 곡 묶음으로 보여 줄 수는 있다. 그러나 작품 묶음과 동일 청취 중복 제거에 쓰는 녹음 식별은 분리해야 한다. 라이브와 원곡을 같은 순위 묶음으로 보고 싶다는 요구가 같은 시각 기록을 중복으로 지워도 된다는 뜻은 아니다.

## 후속 검수

### 상세 청취에서 확인한 표기 차이와 충돌

Standing Next to You의 동일 제목에 앨범 `GOLDEN`과 `GOLDEN [Explicit]`가 있다. 각각 원곡 MBID `4a89a693-a5a7-4348-9cf6-718aad1667fb`가 붙은 관측 조합에서 30,545회와 1,000회를 확인했다. 표기 정리 후보이지만 Explicit 표시만으로 clean 버전과 같은 오디오라고 단정하지 않는다. `Standing Next to You (The Remixes)` 앨범 표기도 함께 있어 원곡 수록 여부와 버전을 별도로 확인해야 한다.

LOST!에는 `Right Place, Wrong Person`과 `(Ver. A)` 앨범 표기가 있다. MBID가 같은 두 관측 조합에서 각각 84,473회와 118회를 확인했다. 같은 앨범명에 MBID 없이 다른 MSID로 남은 53,517회 조합도 있다. MBID만 이용하면 연결하지 못하는 이름 후보의 실제 사례다. 이 숫자는 특정 MSID와 메타데이터 조합의 횟수이며 곡 전체 횟수가 아니다.

Bad Blood는 특히 연결 충돌을 주의해야 한다. 공식 API에서 2024년 라이브를 가리키는 `7fad1aa3-d874-48d8-842a-9ca9afea068e`가 로컬 앨범 `1989` 조합에 534회, `1989 (Deluxe Edition)`에 307회 붙어 있었다. 따라서 이 행들을 실제 라이브 청취로 확정하지 않는다. 공식 녹음 ID가 가리키는 버전과 수집된 앨범명이 충돌한다는 사실을 먼저 검수한다.

### 이름만으로 합쳤을 때의 조건부 영향

각 후보 MSID의 사전 변형이 같은 정규화 이름 쌍을 가지는지 먼저 확인했다. 해당 MSID의 상세 전체를 조회하고 Python 집합으로 사용자와 시각의 고유 조합을 다시 셌다. 아래는 이름 일치를 같은 녹음으로 가정한 민감도 측정이며 승인된 삭제량이 아니다. 위처럼 다른 버전과 오연결이 섞일 수 있다.

| 이름 후보 | MSID | 고유 계정 | 현재 횟수 | 같은 시각 초과 | 가정 적용 후 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Daft Punk - Get Lucky | 76 | 339 | 756 | 7 | 749 |
| Jung Kook - Standing Next to You | 46 | 831 | 79,706 | 8,942 | 70,764 |
| Radiohead - Reckoner | 80 | 836 | 5,162 | 57 | 5,105 |
| RM - LOST! | 28 | 561 | 157,584 | 5,721 | 151,863 |
| Taylor Swift - Bad Blood | 78 | 427 | 2,364 | 28 | 2,336 |

다른 아티스트 표기나 제목 접미사가 붙은 같은 작품 전체를 센 수치는 아니다. 앞선 MBID 기준과 후보 범위가 다르므로 감소량을 서로 더하지 않는다. 고유 계정 수는 후보 검수 우선순위에 쓰며, 전체 후보를 청취자 수로 정렬한 순위는 아니다.

우선 같은 녹음으로 볼 근거가 충분한 표기 차이 후보부터 승인한다. 라이브, 리믹스, Drumless와 피처링 버전은 별도 상태로 남긴다. 로컬 이름과 공식 제목 충돌, API 미확인과 여러 MBID 충돌은 수동 검수 대상으로 분리한다. 원본 MSID와 관측 메타데이터는 보존하고, 확정한 매핑에 근거와 규칙 버전을 기록한다. 이 조사에서 Silver 횟수나 Gold를 변경하지 않는다.
