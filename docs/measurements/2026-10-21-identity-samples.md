# MSID와 이름 후보 키의 실제 표본 검수

이번 범위는 표본 검수와 전처리 후보 고정이다. 전체 정확도 비교나 Silver 및 Gold의 키 교체는 실행하지 않았다. 공식 녹음의 구분을 기준으로 보며 Sped Up, Remix, 라이브, Drumless와 clean/explicit은 원곡으로 흡수하지 않는다.

## 입력과 표본의 성격

2026-10-19 월별 Silver v0의 사전에서 Radiohead, Billie Eilish, Beyoncé, Daft Punk, RM, alt-J와 Jung Kook의 선택한 이름 표기를 조회했다. 44,938개의 고유 MSID+이름+관측 MBID 조합에서 목적 표본 8묶음, 29행을 보관했다. 반복 횟수나 전체 실제 곡 개수가 아니다. 익숙한 곡과 알려진 충돌을 선택했으므로 모집단의 대표 표본이 아니며 성공률과 오류율을 산출하지 않는다.

로컬 MusicBrainz 20260905-002519 스냅샷을 기준으로 표본의 현재 녹음 MBID 10개에 대한 제목과 설명을 원래 recording 행과 대조했다. Beyoncé 크레딧도 artist_credit_name과 artist 행까지 따라가 공식 아티스트 ID를 확인했다. 원본 Silver의 두 표 로그 지문은 전후 동일했다. 조사는 읽기 전용이며 전체 후보를 묶거나 청취 횟수를 바꾸지 않았다.

## 확인한 사례

| 기준과 유형 | 실제 표본 | 공식 근거와 판단 |
| --- | --- | --- |
| MSID가 같은 녹음을 나눔 | Radiohead, Reckoner의 MSID `00ff4483-dc8d-447f-b200-565c80347ff8`과 `0ab4e6b3-637d-459c-9ec2-db1d0e8f22e3` | 둘 다 녹음 `d9b46ecb-5472-4dcd-8fa4-dd6723189e27`, 제목 Reckoner, 크레딧 Radiohead. MSID만 쓰면 이 공식 녹음 후보가 나뉜다. |
| 한 MSID에 다른 녹음 ID가 섞임 | Radiohead, Let Down의 MSID `000ff079-136d-4869-ab0e-6885078e2141` | `47b02a82-c3bf-4647-b894-dd1c8f608e7f`, `9e3e014f-14d1-47da-9712-0dc1dfe00421`과 라이브 `e0030e37-586f-40fb-babe-64a2a3279c49`가 함께 관측된다. 마지막은 2006-05-15 Civic Hall 라이브 설명이다. 실제로 다른 오디오가 들어왔는지 MBID가 잘못 붙었는지는 미확정이며, MSID 하나로 버전을 보증할 수 없다는 충돌 증거다. |
| 이름이 다른 버전을 합침 | Daft Punk + Get Lucky라는 동일 로컬 이름 | 기본 `833f00e1-781f-4edd-90e4-e52712618862`와 공식 제목 Get Lucky (Drumless Edition)의 `dbeffc1d-8f04-483e-8d02-270ad4922b28`가 한 이름 후보가 된다. 버전 표기가 로컬 제목에 빠진 경우를 이름 정규화로 해결할 수 없다. |
| 이름에서 라이브가 구분되지 않음 | Radiohead + Reckoner라는 동일 로컬 이름 | 기본 `d9b46ecb-5472-4dcd-8fa4-dd6723189e27`와 From the Basement 라이브 `694065b4-3910-49c8-a4dd-cb5d270201a1`가 함께 있다. 라이브 설명은 2008-06-24 The Hospital Club, London이다. 공식 메타데이터상 별도 녹음이며 로컬 MBID 오연결 가능성은 여전히 검수 대상이다. |
| 기본 이름 처리가 같은 녹음을 나눔 | Beyonce + Irreplaceable과 Beyoncé + Irreplaceable | MSID는 각각 `b6328b51-2b54-4b01-9347-562f5e9bd402`와 `02303829-10e6-49be-8c73-a9e8700d377c`, 녹음은 둘 다 `0a65e058-83c4-4982-9a5e-d2495fc4565c`. 공식 크레딧은 Beyoncé다. 소문자와 공백만 정리하면 이 두 표기는 합쳐지지 않는다. |
| 하이픈 문자 차이로 나뉨 | alt-J 및 alt‐J + Breezeblocks | ASCII 하이픈 표본 MSID `01bb14ab-0f57-44fb-a991-8b6426b62d6c`와 U+2010 하이픈 표본 `049bcc99-897c-4f42-a04f-104e2a8b8fd8`가 같은 녹음 `4a154661-f61a-4ea0-b88f-fe89ae5d3afc`를 가리킨다. 문자 정리 후보로 적합하다. |
| MBID 없는 후보는 정답 미확정 | Beyoncé + Irreplaceable의 MSID `150bce51-5374-40df-bcc2-01f57de50be2` 등 | 이름 후보는 만들 수 있지만 이 기록이 기본 녹음인지 제목에서 누락된 버전인지 공식 연결 근거가 없다. 정규화 성공이나 동일 녹음 확정으로 세지 않는다. |
| Remix 보존 대조군 | Billie Eilish, bellyache (Marian Hill remix) | MSID `2987d2fc-be74-42a2-880b-96165c7c198f` 등은 공식 녹음 `21cdf196-6418-40b9-b633-d5868aa9c727`와 같은 Remix 제목을 가진다. 같은 리믹스 내부의 연결 후보와 원곡 흡수는 다른 작업이다. |

공식 MBID 일치도 원본 오디오를 직접 확인한 증거는 아니다. 다른 MBID의 존재만으로 실제 다른 청취가 확정되지는 않는다. 표본은 각 키가 담지 못하는 정보와 충돌의 유형을 보여 준다.

## 두 번째 단계의 전처리 후보

`sample-name-v1`을 후보 생성 전용으로 구현했다. 원본 표기와 MSID를 보존하며 현재 Silver와 Gold에서는 호출하지 않는다.

1. Unicode NFC로 `é`와 `e + U+0301`처럼 같은 글자의 구성 차이를 맞춘다.
2. 소문자로 바꾸고 앞뒤 공백 및 연속 공백을 정리한다. NBSP와 탭도 공백으로 처리한다.
3. U+2018/U+2019 따옴표와 U+2010/U+2011 하이픈을 대응하는 ASCII 문자로 맞춘다.
4. 이번에 공식 크레딧으로 확인한 Beyonce/Beyoncé만 별칭 후보 사전에서 아티스트 `859d0860-d480-4efd-970c-c05d5f1776b8`로 연결한다.
5. 곡 제목의 괄호와 버전 및 피처링 표기는 유지하고 곡 제목의 악센트도 제거하지 않는다.

NFC는 악센트를 지우지 않으므로 Beyonce와 Beyoncé의 차이를 없애지 않는다. 모든 이름에서 악센트를 제거하면 Résumé와 Resume 같은 서로 다른 이름도 같아질 수 있어 이번에는 적용하지 않았다. 아티스트 이름만으로 만든 별칭 키는 후보이며, 같은 이름을 가진 다른 인물까지 공식 귀속한 것으로 보지 않는다.

따라서 이름 키는 연결 후보를 찾는 데 유용하지만 그 자체가 최종 곡 식별자가 되지는 않는다. 이번 표본에서 MSID도 버전의 정답을 보증하지 않았다. 다음 전수 실험 여부와 비교 기준은 이 사례를 검토한 뒤 결정한다.

## 검증과 재현

`tests/test_identity_sample_review.py`에서 모듈 부재 실패를 먼저 확인한 뒤 NFC 구성 차이, 확인된 아티스트 별칭, 버전 표기 보존, 미확인 악센트 보존, 문자열 경계와 결측 이름 거절을 검사했다. 새 테스트 3개가 통과했다. 준비 자료를 재사용하는 표본 추출은 다음과 같다.

```powershell
$env:PYTHONPATH='src'
venv/Scripts/python scripts/investigations/review_identity_samples.py --prepared data/song-identifiers-20261021 --silver C:/Users/SSAFY/Desktop/jjh/flux-data/monthly-silver-20261019 --output <새표본경로>
```

`--prepared`는 앞선 준비 단계에서 저장한 공식 녹음과 ID 해석 표를 담은 로컬 DB다. 앞선 전수 비교는 준비 SQL 오류로 종료되어 결과가 없으며 이번에 재실행하지 않았다. 준비 DB의 전체 실험 완료 여부에 의존하지 않고, 이번 표본은 원래 공식 recording 행 및 아티스트 크레딧을 다시 확인했다. 현재 결과는 `data/identity-sample-review-20261021/result.json`이며 completed와 source_unchanged가 참이다. 원본과 조사 원자료는 git에 포함하지 않는다.
