# 비교용 이름의 라틴 악센트 제거 재검수

원문 이름을 보존하고 아티스트명과 곡명의 비교용 값에 라틴 악센트 제거를 추가했다. [이전 전처리 표본](2026-10-21-name-preprocessing-preview.md)의 42행을 같은 순서로 다시 처리했다. 새 표본을 추가하거나 전체 음악 데이터를 전처리하지 않았다. 원문과 Silver 및 Gold의 키, play_count는 유지했다.

## 바뀐 규칙

후보 함수 버전은 `sample-name-v2-latin-accents`, 정책은 `name-preprocessing-registry-v2`, Unicode는 16.0.0이다. 비교 문자열의 순서는 NFC, 소문자, 라틴 악센트 제거, 승인된 문자 치환과 공백 압축이다. 같은 단계 목록으로 실제 비교 값과 변경 이력을 만든다.

악센트 단계는 NFD로 분해한 문자열에서 라틴 글자에 이어진 결합 부호 중 결합 클래스가 0이 아닌 것을 제외하고 NFC로 다시 조합한다. 다른 문자 체계의 악센트를 일괄 제거하지 않으며 클래스가 0인 variation selector와 결합 제어자, ZWJ는 이 단계에서 보존한다. 이는 음악 비교용 정책이며 서로 다른 이름이 같은 아티스트나 녹음이라는 보장은 아니다.

`Beyoncé`, `Beyoncé`, `Beyonce`의 비교 문자열은 모두 `beyonce`다. `œ → oe`, `ø → o`, `ß → ss`, NFKC 전역 적용은 승인하지 않았다. Remix, Sped Up, Live, Clean/Explicit와 피처링 문구는 유지한다. `İ`는 소문자와 악센트 제거를 거쳐 `i`가 되고 `ı`는 유지된다. 언어별 이름 동등성은 별도 검수 대상이다.

공식 아티스트 별칭 조회는 악센트 제거 전 철자로 한다. 기존 확인된 Beyonce와 Beyoncé는 같은 공식 아티스트 후보 ID를 유지한다. 테스트용 미검수 철자 `Beyoncè`도 비교 문자열은 beyonce가 되지만 그것만으로 공식 ID를 부여하지 않는다. 이 테스트용 철자가 실제 표본에서 발견됐다는 뜻은 아니다.

## 실제 전후

| 표본의 원래 아티스트와 곡명 | v1 비교 문자열 | v2 비교 문자열 |
| --- | --- | --- |
| `Beyoncé` + `Heaven` | `beyoncé` + `heaven` | `beyonce` + `heaven` |
| `Beyoncé` + `ENERGY` | `beyoncé` + `energy` | `beyonce` + `energy` |
| `Beyoncé` + `Irreplaceable` | `beyoncé` + `irreplaceable` | `beyonce` + `irreplaceable` |
| `Beyonce` + `Irreplaceable` | `beyonce` + `irreplaceable` | `beyonce` + `irreplaceable` |
| `Daft Punk` + `Phœnix` | `daft punk` + `phœnix` | `daft punk` + `phœnix` |
| `Billie Eilish` + `bellyache (Marian Hill remix)` | `billie eilish` + `bellyache (marian hill remix)` | `billie eilish` + `bellyache (marian hill remix)` |

이전 실행 대비 아티스트 문자열이 바뀐 것은 6행이다. Heaven 1행, ENERGY 1행, Irreplaceable 4행이며 곡명 문자열이 추가로 바뀐 행은 0행이다. 원문에서 시작한 변경 이벤트는 소문자 84개, NFC 2개, 악센트 제거 6개, 승인 문자 치환 6개와 공백 압축 2개다. 각각 필드와 단계별 사건 수이므로 더해서 곡 수나 청취 수로 사용하지 않는다.

문자열만 사용한 `(아티스트, 곡명)`의 서로 다른 키는 표본에서 17개에서 16개가 됐다. 새로 같아진 행 쌍은 Irreplaceable의 12쌍이다. 이 중 2쌍은 양쪽이 같은 공식 녹음 `0a65e058-83c4-4982-9a5e-d2495fc4565c`를 공유하고 10쌍은 최소 한쪽의 공식 녹음 ID가 없다. 12개의 새 곡이나 12회의 중복 청취를 발견했다는 뜻이 아니다. 같은 7행 사이의 쌍을 센 수치다.

기존의 확인된 아티스트 별칭 ID까지 반영하는 최종 후보 키는 바뀐 행이 0행이며 그룹 구성도 그대로다. v1부터 Beyonce와 Beyoncé를 같은 공식 아티스트 후보 ID로 연결했기 때문이다. 문자 비교와 실제 대상의 후보 키를 구분해 효과를 계산했다.

## 잘못 합칠 위험

이번 악센트 제거가 새로 만든 공식 녹음 참조 충돌은 문자열 키와 최종 후보 키 모두 0쌍이다. 표본의 악센트 관련 사례는 Beyoncé에 치우쳐 있고 곡명 악센트 변화는 관측되지 않았다. 따라서 악센트 제거가 전역적으로 안전하다는 뜻으로 해석하지 않는다. MBID 없는 행도 이름만으로 자동 승인하지 않았다.

기존 같은 최종 후보 키의 8개 그룹은 그대로 남는다. 5개는 여러 공식 녹음 참조가 있는 그룹, 2개는 같은 참조만 있는 그룹, 1개는 같은 참조와 MBID 없는 행이 섞인 그룹이다. 실제 버전 차이와 원본 MBID 오류 가능성을 확인하기 전에는 병합하지 않는다.

| 실제 후보 | 남아 있는 문제 | 처리 |
| --- | --- | --- |
| Radiohead + I Can't | 기본 녹음과 1992-06-22 BBC Maida Vale 라이브가 같은 로컬 이름 | 버전 확인 전 병합 보류 |
| Radiohead + Bullet Proof..I Wish I Was | 기본 녹음과 1996-04-13 Avalon 라이브 | 같은 이름으로 확정하지 않음 |
| Daft Punk + Get Lucky | 기본 녹음과 Drumless Edition | 다른 버전 유지 |
| Radiohead + Reckoner | 기본 녹음과 From the Basement 라이브 | 다른 버전 유지 |
| Radiohead + Let Down | 기존 한 MSID의 여러 MBID 충돌 | 원본 연결 검수 유지 |

별도로 만든 합성 테스트에서는 Résumé와 Resume, Café와 Cafe가 같은 비교 값이 되면서 서로 다른 참조를 가진 경우 충돌로 표시되는지 확인했다. 이는 실패 가능성을 검사하기 위한 입력이며 실제 데이터 사례나 측정한 오병합률이 아니다. 원래 이름과 버전 근거를 남겨야 이 차이를 다시 판단할 수 있다.

## 검증과 재현

공식 녹음 17개의 제목과 설명을 원래 recording 표에서 읽어 다시 대조했다. 표본 입력 두 파일과 이전 실행 rows.json의 SHA-256, Silver 두 표의 로그 지문이 실행 전후 같았다. 원문 42행의 내용과 순서도 이전 실행과 직접 비교했다. `source_unchanged=true`, `production_applied=false`, `full_corpus_scanned=false`다.

관련 테스트 28개가 통과했다. 악센트 제거의 실패를 먼저 확인했고, 원문 보존, 재적용 결과의 동일성, 다른 문자 체계와 전사 및 버전 표기 보존, 미검수 철자의 공식 ID 자동 부여 방지, 변경 이력과 실제 함수의 일치, 새 충돌 분류를 검사했다. 파일 진입점은 기준 결과 비교, 덮어쓰기와 오래된 입력 차단도 검사한다. JSON의 배열과 메모리의 튜플 차이를 실제 이름 변경으로 세지 않도록 저장 후 재조회도 확인했다. Spark 및 전체 코퍼스 실험은 실행하지 않았다.

```powershell
$env:PYTHONPATH='src'
venv/Scripts/python -m pytest tests/test_name_preprocessing_preview.py tests/test_identity_sample_review.py tests/test_name_character_catalog.py tests/test_artist_attribution.py tests/test_monthly_gold.py -q
venv/Scripts/python scripts/investigations/preview_name_sample.py --pool data/identity-samples-20261021/targeted-evidence.parquet --review data/identity-sample-review-20261021/result.json --prepared data/song-identifiers-20261021 --silver C:/Users/SSAFY/Desktop/jjh/flux-data/monthly-silver-20261019 --baseline data/name-preprocessing-preview-20261021-final/rows.json --output <새출력경로>
```

최종 출력은 `data/name-preprocessing-accents-20261021-v2`다. before-after.md에 42행 전체 전후, rows.json에 원문과 단계별 변경, result.json의 baseline_comparison에 문자열 및 최종 후보 키 변화와 새로 같아진 쌍을 보관했다. v1 결과를 덮어쓰지 않았고 전수 적용은 계속 보류한다.

후속 [다른 아티스트의 102행 확대 검수](2026-10-21-accent-expansion.md)에서는 같은 녹음으로 확인되는 표기 쌍과 새 버전 참조 충돌을 함께 확인했다. 위의 새 충돌 없음은 이 문서의 기존 42행 범위에만 해당한다.
