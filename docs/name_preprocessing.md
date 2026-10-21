# 이름 전처리 매칭표

MSID와 이름 기준을 비교하기 전에 사용하는 문자 대조 사전이다. 원래 이름은 보존하고 비교용 값 및 검수 후보를 따로 만든다. 이 표에 변환 후보가 있다는 사실은 같은 아티스트나 같은 녹음을 승인했다는 뜻이 아니다. 현재 Silver와 Gold의 키 및 횟수는 바꾸지 않았다.

## 기준과 범위

실행 Python의 `unicodedata.unidata_version`은 16.0.0이다. [Python 문서](https://docs.python.org/3.14/library/unicodedata.html)와 [Unicode 16.0.0 UCD](https://www.unicode.org/Public/16.0.0/ucd/ReadMe.txt)를 기준으로 버전을 고정했다. 최신 Unicode와 같다고 가정하지 않는다. 규칙 버전은 `name-preprocessing-registry-v1`이다.

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
| 라틴 악센트 | é/è/ê/ë/ē/ě/ẽ/ę, ñ/ç/ğ/ș | 악센트 제거 결과를 후보 컬럼에만 기록 |
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

검수하는 규칙은 `src/flux/silver/name_matching_rules.json`에 둔다. 25개 유형과 승인된 문자 치환 4개, Beyoncé 별칭 및 그 근거가 들어 있다. 기존 `name_candidates.py`는 이 파일에서 승인된 치환과 별칭만 읽는다. 후보 함수의 기존 결과는 유지한다.

전체 생성 결과는 로컬 `data/name-character-catalog-20261021-final`에 둔다.

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
