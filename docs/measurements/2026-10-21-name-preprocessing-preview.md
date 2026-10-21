# 표본 이름의 전처리 전후와 충돌 검수

문자 매칭표를 음악 데이터 표본에만 적용했다. 기존 8묶음의 표본과 같은 아티스트 범위에서 보관한 표본 풀의 실제 표기 변형을 골라, 중복 관측을 제외한 42행을 비교했다. Unicode 구성, 공백, 아포스트로피, 하이픈, 합자와 호환 문자를 의도적으로 포함한 목적 표본이다. 전체 데이터의 변화량이나 정확도를 추정하지 않는다.

승인된 기존 처리와 검수용 가상 변환을 나눴다. 승인 처리는 NFC, 소문자, 승인 문자 네 개, 공백 압축과 확인된 Beyoncé 별칭이다. NFKC, casefold와 라틴 전사는 각각 독립적인 review_only 결과다. 서로 연달아 적용한 하나의 광범위 변환이 아니다. Silver와 Gold의 이름, 키와 play_count에는 반영하지 않았다.

## 실제 이름은 어떻게 바뀌었는가

아래의 오른쪽 값은 저장할 공식 이름이 아니라 비교용 문자열이다. 표시용 원문은 모두 보존한다.

| 실제 원문 | 승인된 비교용 결과 | 적용 내용 |
| --- | --- | --- |
| `Beyoncé` + `Heaven` | `beyoncé` + `heaven` | e+U+0301을 é로 NFC 합성하고 소문자화 |
| `Beyonce` + `Irreplaceable` | 문자 값은 `beyonce` + `irreplaceable`, 후보 키는 Beyoncé 공식 아티스트 ID + `irreplaceable` | 악센트 없는 원문을 덮어쓰지 않고 확인된 별칭으로 후보를 연결 |
| `alt‐J` + `Breezeblocks` | `alt-j` + `breezeblocks` | U+2010 하이픈과 대소문자 정리 |
| `Radiohead` + `I Can’t` | `radiohead` + `i can't` | U+2019 아포스트로피와 대소문자 정리 |
| `Daft Punk` + `Get Lucky  (feat. Pharrell Williams and Nile Rodgers)` | `daft punk` + `get lucky (feat. pharrell williams and nile rodgers)` | 연속 공백 두 개를 하나로 압축, 참여자 문구 유지 |
| `Daft Punk` + `Technologic  (Digitalism Remix)` | `daft punk` + `technologic (digitalism remix)` | 공백 압축, Remix 유지 |
| `Billie Eilish` + `bellyache (Marian Hill remix)` | `billie eilish` + `bellyache (marian hill remix)` | 대소문자만 정리하고 리믹스 구분 유지 |
| `RM` + `ㅠㅠ (Credit Roll)` | `rm` + `ㅠㅠ (credit roll)` | 한글 호환 자모와 괄호 유지 |

42행 모두 대소문자 변화가 있었다. 세부 변경은 아티스트와 제목을 따로 센 이벤트 수로 소문자 84개, NFC 2개, 승인 문자 치환 6개와 공백 압축 2개다. 이를 서로 더해 변경된 청취 수나 곡 수로 사용하지 않는다. 공식 녹음 ID를 확인할 수 없는 행은 8행이며 이름 정리가 된 것과 녹음이 확인된 것은 다르다.

## 승인하지 않은 추가 변환은 어떻게 달라지는가

| 원문 | 승인된 결과 | 가상 변환 후보 | 판단 |
| --- | --- | --- | --- |
| `Phœnix` | `phœnix` | 라틴 전사 `phoenix` | 실제 `Phoenix` 표본과 같은 공식 녹음 MBID다. 이 표본의 연결 근거는 있으나 œ 전체에 대한 자동 치환 승인은 아님 |
| `Bullet Proof․․I Wish I Was` | `bullet proof․․i wish i was` | NFKC `bullet proof..i wish i was` | U+2024 두 개가 ASCII 마침표 두 개로 바뀜. 같은 공식 기본 녹음 표본과 맞지만, 같은 이름에 라이브도 있어 버전 검수가 필요 |
| `ㅠㅠ (Credit Roll)` | `ㅠㅠ (credit roll)` | NFKC `ᅲᅲ (credit roll)` | U+3160 두 개가 U+1172 두 개로 바뀜. 이번 표본에서 오병합은 관측되지 않았지만 의도한 이모티콘 표기를 바꾸므로 광범위 NFKC는 보류 |

Phœnix와 Phoenix의 실제 MSID는 `063e4b18-38d2-4908-84e1-50e0c60a4fa4`, `05c4c5ec-aaab-4882-b271-1f87c3a495c6`이고 공식 녹음은 둘 다 `28651acc-1f13-4431-b2de-d93a33a50084`다. NFKC만으로 œ가 oe가 되지는 않으며 이번 전사는 별도 검수 후보 규칙이다.

이번 표본에서 casefold는 승인된 소문자 처리보다 추가로 바꾸는 값이 없었다. ß나 zero-width 문자 등 사전에 있는 모든 유형이 이 표본에 관측된 것은 아니다. 이 부분에 합성 데이터를 실제 사례처럼 끼워 넣지 않았다.

## 잘못 합쳐질 수 있는 그룹

| 전처리 후 같은 후보 키 | 공식 자료에서 확인한 차이 | 검수 결과 |
| --- | --- | --- |
| `radiohead` + `i can't` | 기본 녹음 ID와 BBC Maida Vale의 1992-06-22 라이브 ID `61c9cb8e-1b43-4bad-a1fc-bcbd1243e297`가 같은 그룹 | 따옴표 통일은 표기 비교에 유용하지만 곡 병합은 버전 검수 전까지 보류 |
| `radiohead` + `bullet proof..i wish i was` | 기본 `3dbd1679-8988-4859-9879-a570e87edc47`과 1996-04-13 Avalon 라이브 `b6fe3e3b-6f30-4b1d-8057-a684835ee259`가 공존 | 점 문자를 정리해도 동명 라이브는 해결되지 않음 |
| `daft punk` + `get lucky` | 기본 녹음과 Drumless Edition | 버전 정보가 로컬 이름에 없으므로 자동 병합 보류 |
| `radiohead` + `reckoner` | 기본 녹음과 From the Basement 라이브 | 같은 이유로 자동 병합 보류 |
| `radiohead` + `let down` | 기존 한 MSID의 여러 MBID 충돌 | 전처리와 별개인 원본 연결 충돌을 계속 보류 |

승인 처리 후 여러 행이 모이는 후보 그룹은 8개였다. 이 중 5개는 여러 공식 녹음 ID를 가진 검수 대상, 2개는 표본 내 같은 공식 ID 후보, 1개는 같은 공식 ID와 MBID 없는 행이 함께 있는 그룹이다. 이는 충돌이 있는 곡을 일부러 넣은 목적 표본의 분포이며 오병합률이 아니다. 이전부터 원래 이름이 같던 충돌도 포함하므로 이번 문자 변환 때문에 새로 발생한 오류 5건이라고 해석하지 않는다. 다른 공식 MBID도 실제 다른 오디오가 아니라 원본의 잘못된 MBID일 수 있다.

Beyoncé의 Irreplaceable 후보에는 확인된 동일 녹음과 MBID 없는 기록이 함께 있다. 별칭으로 아티스트 후보를 연결했다고 미확인 행까지 기본 녹음으로 확정하지 않는다. alt-J의 Breezeblocks 표기 변형은 표본에서 같은 공식 ID를 공유했고 비교용 문자는 의도대로 맞았다.

## 검증과 재현

선택한 공식 녹음 17개의 제목과 설명을 원래 MusicBrainz recording 행에서 다시 확인했다. Silver 두 표의 로그 지문과 표본 입력 두 파일의 해시는 실행 전후 같았다. 원본 및 실제 집계는 변경하지 않았다.

새 모듈 부재로 테스트가 실패하는 것을 확인한 뒤 전처리 추적을 구현했다. 테스트는 원문 보존, 단계별 변경 기록, 아티스트 문자 값과 별칭 ID의 분리, 검수용 변환의 비적용, 버전 구분 유지, MBID 충돌 및 미확인 행 판정, 실제 파일 출력, 덮어쓰기와 오래된 입력 차단을 검사한다. 입력 파일의 부모 폴더를 지나치게 넓게 차단하던 경로 검사도 작은 파일 테스트에서 발견해 파일과 디렉터리를 구분하도록 수정했다. 관련 테스트 19개가 통과했다.

```powershell
$env:PYTHONPATH='src'
venv/Scripts/python scripts/investigations/preview_name_sample.py --pool data/identity-samples-20261021/targeted-evidence.parquet --review data/identity-sample-review-20261021/result.json --prepared data/song-identifiers-20261021 --silver C:/Users/SSAFY/Desktop/jjh/flux-data/monthly-silver-20261019 --output <새표본결과경로>
```

현재 출력은 `data/name-preprocessing-preview-20261021-final`이다. `before-after.md`는 42행 전체 전후 표, `rows.json`은 원문과 단계별 변경 및 승인/검수 후보, `result.json`은 입력 해시와 그룹별 참조 상태다. 전체 이름을 비교한 전수 실험과 전수 전처리 적용 여부는 이 표본을 검토한 다음 결정한다.
