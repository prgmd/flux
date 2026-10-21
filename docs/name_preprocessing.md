# 이름 전처리 매칭표

MSID와 이름 기준을 비교하기 전에 사용하는 문자 대조 사전이다. 원래 이름은 보존하고 비교용 값 및 검수 후보를 따로 만든다. 이 표에 변환 후보가 있다는 사실은 같은 아티스트나 같은 녹음을 승인했다는 뜻이 아니다. 현재 Silver와 Gold의 키 및 횟수는 바꾸지 않았다.

## 기준과 범위

실행 Python의 `unicodedata.unidata_version`은 16.0.0이다. [Python 문서](https://docs.python.org/3.14/library/unicodedata.html)와 [Unicode 16.0.0 UCD](https://www.unicode.org/Public/16.0.0/ucd/ReadMe.txt)를 기준으로 버전을 고정했다. 최신 Unicode와 같다고 가정하지 않는다. 현재 규칙은 `name-preprocessing-registry-v2`, 표본 비교 함수는 `sample-name-v2-latin-accents`다. v1의 문자표 생성 결과는 당시 정책 사본과 함께 보존하고, v2는 [같은 42행의 악센트 제거 재검수](measurements/2026-10-21-name-accents.md)에 적용했다.

Unicode 스칼라 값 1,112,064개를 순회해 다음 범위의 관련 문자 21,708개를 대조했다.

- 공식 Scripts와 ScriptExtensions에서 Latin 또는 Latn을 가지는 1,555개 코드 포인트 전부.
- 문자 이름에 LATIN이 있는 글자와 대소문자 변환 또는 NFC/NFKC 결과가 달라지는 문자.
- 모든 결합 부호, 구두점, 기호, 숫자, 구분자, 제어 문자와 형식 문자.
- Unicode 16.0.0 confusables 파일에 있는 유사 문자 관계 6,355개 전부의 원문자.

미할당, 서로게이트와 사적 사용 문자는 동등 관계를 부여하지 않고 기본 보존 대상으로 둔다. 나머지 포함되지 않은 문자도 기본 보존한다. 임의 길이의 모든 문자열, 모든 아티스트의 별칭과 모든 언어의 전사 관계를 완성했다는 뜻은 아니다. 한글 자모와 emoji의 조합은 단일 문자 표와 별도로 정규화 검증 사례 및 보존 예시에 포함한다. 유사 문자 관계는 [Unicode의 관련 자료](https://www.unicode.org/Public/security/16.0.0/confusables.txt)를 검수 후보로 보관하며 자동 치환하지 않는다.

## 유형별 처리표

| 유형 | 대조하는 예 | 기본 처리 |
| --- | --- | --- |
| 정준 구성 차이 | é와 e+U+0301, 가와 U+1100+U+1161 | 비교용 NFC |
| 대소문자 | A/a, É/é | 기존 비교용 소문자 규칙 유지 |
| casefold 확장 | ß/ss, ſ/s, Σ/σ/ς | 별도 후보, 자동 적용 보류 |
| 언어별 대소문자 | I/i/İ/ı | 언어와 별칭 검수 필요 |
| 라틴 악센트 | é/è/ê/ë/ē/ě/ẽ/ę, ñ/ç/ğ/ș | 표본의 아티스트 및 곡 비교용 값에서 제거, 원문과 운영 데이터 보존 |
| 합자와 전사 | æ/ae, œ/oe, ø/o, ł/l, þ/th | 개별 검수, 전역 동등 처리 안 함 |
| 결합 부호 순서 | a+acute+dot와 a+dot+acute | 정준적으로 같은 경우만 NFC |
| 전각과 반각 | Ａ/A, ａ/a, １/1, ＆/& | NFKC 후보, 자동 적용 보류 |
| 장식 문자와 호환형 | 𝐀/A, ⓐ/a, ﬁ/fi, K/K, Å/Å | NFC 결과와 NFKC 후보를 구분 |
| 공백 | 일반 공백, 탭, 줄바꿈, NBSP, 좁은 NBSP, 전각 공백 | 비교용 공백 통일과 압축 |
| 아포스트로피 | ASCII와 U+2018/U+2019, U+02BC, grave, acute | 승인된 U+2018/U+2019만 통일, 나머지 검수 |
| 큰따옴표 | ASCII, curly, guillemet | 원형 보존과 검수 |
| 하이픈과 대시 및 마이너스 | U+002D, U+2010/U+2011, figure/en/em dash, minus | 승인된 U+2010/U+2011만 하이픈으로 통일 |
| 구두점과 괄호 | 마침표/줄임표, slash 변형, (), [], {} | 삭제하지 않음 |
| 의미를 가진 기호 | &, and, +, !, $, ×, x, #, @ | 서로 자동 치환하거나 삭제하지 않음 |
| 숫자 표현 | 1/①/¹, Ⅳ/IV, 아라비아 숫자 | 후보만 기록, 원래 구분 보존 |
| 보이지 않는 문자 | zero-width space, ZWNJ, ZWJ, BOM, soft hyphen | 위치와 코드 포인트를 표시, 일괄 삭제 안 함 |
| 양방향 제어 문자 | embedding, override, isolate | 표시 및 검수, 원문 보존 |
| variation selector와 emoji | ❤/❤️, 👩‍🎤, 피부색 변형 | 조합과 선택자 보존 |
| 다른 문자 체계의 닮은 글자 | Latin A, Cyrillic А, Greek Α | 시각적 유사 후보만 표시 |
| HTML 및 URL 표기 | &amp;, &#39;, %20 | 수집 경로의 인코딩 확인 후 판단 |
| 깨진 인코딩 | BeyoncÃ©, â€™ | 임의 복구하지 않고 별도 검수 |
| 아티스트 별칭 | Beyonce/Beyoncé, Jung Kook/Jungkook | 공식 ID로 확인한 별칭만 후보 사전에 승인 |
| 곡 버전과 참여자 | Remix, Sped Up, Slowed, Live, Clean, Explicit, feat., ft. | 제목에서 제거하지 않음 |
| 그 밖의 문자 체계 | 한글, 일본어, 키릴 문자, 아랍 문자 | 원형 보존, 무조건 로마자화하지 않음 |

장식 문자처럼 보여도 NFC에서 이미 같은 문자로 정규화되는 경우가 있다. 따라서 형태만 보고 규칙을 결정하지 않고 문자별 실제 NFC, NFKC, lower와 casefold 결과를 함께 저장한다. `ß → ss`와 `ø → o`는 서로 성격도 다르다. 전자는 casefold 결과이고 후자는 프로젝트가 제안한 전사 후보이며 Unicode가 같은 문자라고 정한 관계가 아니다.

## 매칭표의 파일과 필드

검수하는 규칙은 `src/flux/silver/name_matching_rules.json`에 둔다. 25개 유형과 승인된 문자 치환 4개, 라틴 악센트 처리 범위, Beyoncé 별칭 및 그 근거가 들어 있다. `name_candidates.py`의 비교 문자열에는 NFC, 소문자, 라틴 악센트 제거, 승인 문자 치환과 공백 압축을 순서대로 적용한다. 같은 단계 목록을 미리보기 이력에도 사용한다. 악센트 제거는 NFD로 분해한 뒤 라틴 글자에 이어진 정준 결합 부호를 제거하고 NFC로 다시 조합한다. 결합 클래스가 0인 선택자와 형식 문자는 이 단계에서 지우지 않으며 다른 문자 체계는 전사하지 않는다.

별칭 조회는 악센트 제거 전 철자를 사용한다. 확인된 Beyonce와 Beyoncé는 같은 공식 아티스트 후보 ID를 유지하지만, 미검수 철자가 단지 beyonce가 되었다고 공식 ID를 새로 부여하지 않는다. 곡명의 Café와 Cafe도 비교 키는 같아질 수 있으므로 같은 녹음의 승인은 별도다. `œ → oe`, `ø → o`와 `ß → ss`는 이번 승인 범위에 포함하지 않는다. 소문자 및 악센트 단계 때문에 `İ → i`는 되지만 `ı`는 보존한다. 언어별 철자 전체의 동등성을 승인한 것은 아니다.

v1 전체 문자표 생성 결과는 로컬 `data/name-character-catalog-20261021-final`에 둔다. 아래 수치와 그 안의 policy.json은 v1 실행 기록이며 v2 정책으로 다시 생성했다고 해석하지 않는다.

| 파일 | 내용 |
| --- | --- |
| matching-table.md | 사람이 읽는 25개 유형과 예시의 코드 포인트 |
| characters.jsonl | 문자 21,708개, 이름과 범주, NFC/NFD/NFKC/NFKD, lower/upper/casefold, 승인 치환과 검수 후보 |
| confusables.json | 공식 유사 문자 관계 6,355개, 모두 review_only |
| latin_candidates.json | 라틴 문자에서 제안되는 37개 ASCII 후보 그룹. 채택된 동일 문자 그룹이 아님 |
| policy.json | 이번 생성에 사용한 규칙 사본 |
| result.json | Unicode 및 정책 버전, 출처 URL과 SHA-256, 범위와 검증 결과 |

라틴 전사 후보가 없는 글자는 값을 추정하지 않고 null로 남긴다. 사전에 없는 글자나 문자열은 보존한다. 새로운 아티스트 별칭은 같은 공식 대상이라는 근거를 추가해야 하며, 문자 유사성만으로 공식 아티스트를 귀속하지 않는다.

## 검증과 재현

공식 NormalizationTest.txt 19,965개 사례에 대해 NFC/NFD/NFKC/NFKD 조건 399,300개가 모두 일치했다. 이것은 Python 정규화 함수와 고정한 Unicode 자료가 맞는지 확인한 수치이며 곡 매칭 정확도가 아니다. 라틴 문자 범위 1,555개가 빠짐없이 문자표에 포함되는지도 별도로 검사했다.

단위 테스트는 모듈 부재 실패부터 확인했다. 악센트와 합자, 언어별 대소문자와 전각 문자, 구두점 및 보이지 않는 문자 보존, 다른 문자 체계 미전사, 범위 파싱과 규칙 버전, 잘못된 정규화 기대값 탐지, 결과 덮어쓰기 차단과 자료 버전 불일치를 검사한다. 기존 후보 키와 Gold 테스트를 함께 실행해 15개가 통과했다.

```powershell
$env:PYTHONPATH='src'
venv/Scripts/python scripts/investigations/build_name_character_catalog.py --sources data/unicode-16.0.0 --output <새매칭표경로>
```

입력은 Unicode 16.0.0의 Scripts.txt, ScriptExtensions.txt, NormalizationTest.txt와 security/16.0.0/confusables.txt다. Python의 UCD 버전이나 입력 파일 헤더 버전이 다르면 중단한다. 출력이 이미 있어도 덮어쓰지 않는다. 이번 생성은 음악 코퍼스를 읽지 않았고 Silver나 Gold를 재생성하지 않았다. 다음 단계는 이 매칭표에 실제 관측 빈도와 충돌 사례를 붙여 승인 범위를 보강한 후, 고정한 전처리 기준으로 MSID와 이름의 정확도를 비교하는 것이다.


## 실제 표본에 적용한 전후 비교

[42행 전후 비교와 충돌 검수](measurements/2026-10-21-name-preprocessing-preview.md)에서 승인 규칙과 검수 후보를 나눠 적용했다. 원래 이름과 공식 녹음 근거를 보존했고, 이름이 같아져도 여러 버전이 있으면 병합을 보류했다. 전수 적용 여부는 이 결과 검토 이후다.

이후 같은 표본을 [악센트 제거 포함 v2](measurements/2026-10-21-name-accents.md)로 다시 처리했다. 아티스트 비교 값 6행이 바뀌었고 기존 별칭 ID를 포함한 후보 키는 유지됐다. 새 참조 충돌은 없었지만 기존 버전 충돌과 MBID 없는 행은 계속 보류한다. 관련 테스트 28개가 통과했으며 전체 음악 코퍼스와 Spark는 실행하지 않았다.

[Beyoncé 밖의 확대 검수](measurements/2026-10-21-accent-expansion.md)에서는 제한한 아티스트 범위에서 16개 곡 후보 묶음과 대조 행을 포함한 102행을 확인했다. Céline Dion과 L'Impératrice 등의 같은 공식 녹음 표기 쌍이 있었고, Jóga와 Glósóli 등에서는 라이브 및 믹스 참조가 새로 같은 이름 키에 연결되는 위험도 확인했다. bôa와 BoA는 이름 키만 같아지는 서로 다른 공식 아티스트였다. 문자 전처리 결과만으로 병합하지 않으며 전수 적용은 계속 보류한다.

연결 허용 조건은 [R9 공식 ID 규칙](recording_link_rules.md#r9-공식-id를-확인한-악센트-후보-연결)을 따른다. 악센트 제거는 후보 탐색에만 사용하며, 양쪽의 검수한 공식 아티스트 ID와 현재 녹음 MBID 및 확인된 버전이 모두 같아야 연결 가능이다. 미확인 행에 ID를 전파하거나 Live, Remix, Sped Up을 일반 녹음에 흡수하지 않는다. 현재는 독립 판정 함수와 테스트만 추가했고 실제 데이터 연결은 보류한다.
