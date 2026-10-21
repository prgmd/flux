# Beyoncé 밖의 악센트 표본과 오병합 위험

이전 42행에서는 악센트 변화가 Beyoncé에 치우쳐 있었다. 이번에는 Beyoncé를 제외하고 아티스트 및 곡명의 악센트 사례를 늘렸다. 비교용 이름이 같아진다는 사실과 같은 녹음으로 합쳐도 된다는 판단을 분리했다. 공식 자료에서 버전 및 아티스트 차이가 확인되어 이름 키만으로 자동 병합하지 않는 결론을 유지한다. 운영 데이터와 전처리 정책은 변경하지 않았다.

## 표본 범위와 비교 방법

같은 월별 Silver 버전의 메타데이터에서 미리 고른 아티스트 검색 키 22개를 조회했다. Björk, Sigur Rós, Motörhead, Tiësto, José González, Céline Dion, Zoé, Café Tacvba, ROSALÍA, bôa/BoA, Måneskin, Mylène Farmer, Édith Piaf, Róisín Murphy, L'Impératrice, Ólafur Arnalds, Antonín Dvořák, MØ, BØRNS, Café del Mar, Stromae와 Cœur de pirate 범위다. 검색 단계의 DuckDB strip_accents는 범위를 찾는 용도이며, 실제 비교는 기존 Python의 `sample-name-v2-latin-accents`로 했다.

목적 표본 풀은 고유 메타데이터 조합 52,959행이다. 전체 Silver나 청취 사건 수가 아니다. 이 범위에서 악센트 제거 전 키 둘 이상이 같은 비교 키로 모이는 후보 1,357개를 찾았다. 그중 실제 사례를 고른 16묶음은 원래 1,167행이며, 이전 키 및 녹음 ID 조합별 대표 98행을 검수했다. 여기에 전사 제외 대조 2행과 아티스트 구분 대조 2행을 더해 총 102행이다. 102행 중 녹음 ID가 확인되지 않은 것은 38행이다. 36행은 원래 MBID가 없고 2행은 값이 있지만 준비된 공식 녹음 조회에서 확인되지 않았다. 무작위 표본이나 전체 정확도 추정은 아니다.

NFC, 소문자, 승인 문자와 공백 처리는 양쪽에 같게 적용하고 라틴 악센트 제거의 유무만 비교했다. 제거 전 그룹 사이에서 서로 다른 공식 녹음 참조가 새로 연결되는 경우와, 한 그룹 안에 이미 여러 참조가 있던 경우를 나눴다. 단순히 제거 후 MBID가 여러 개라는 이유로 모두 새 오병합이라고 세지 않았다.

| 제한한 풀의 후보 분류 | 그룹 수 | 해석 |
| --- | ---: | --- |
| 제거 전 서로 다른 키 사이에 녹음 참조 충돌이 새로 연결됨 | 87 | 새로 연결될 위험 후보, 전부 다른 오디오로 확정한 수치 아님 |
| 기존 여러 참조 그룹에 미확인 행이 추가됨 | 140 | 기존 충돌이며 새 악센트 오류로 계산하지 않음 |
| 같은 참조와 미확인 행이 섞임 | 640 | 확인된 쌍과 미확인 행을 별도 판단 |
| 참조가 전부 없음 | 472 | 이름만으로 동일 녹음 확정 불가 |
| 같은 참조만 있음 | 18 | 이 풀에서 관측한 참조 일치 후보 |

이 수치는 고른 아티스트 범위의 후보 분포다. 전체 음악의 오병합률이나 검수 완료 곡 수로 사용하지 않는다. 87개 전체의 사실 검수를 완료한 것도 아니다. 상세 검수 16묶음은 새 참조 충돌 6개, 기존 참조 충돌 2개, 같은 참조와 미확인 행이 섞인 경우 6개, 참조 전무 2개다.

## 같은 녹음으로 확인되는 표기 쌍

다음 여섯 사례는 악센트 표기가 다른 양쪽에서 같은 공식 녹음 ID를 확인했다. 각 묶음에는 MBID 없는 행도 함께 있으므로 확인된 쌍의 근거를 미확인 행 전체에 확장하지 않았다.

| 실제 관측 표기 | 비교용 결과 | 양쪽에서 확인한 녹음 MBID |
| --- | --- | --- |
| Céline Dion + `Parler à mon père` / `Parler a Mon Pere` | `celine dion` + `parler a mon pere` | `33dd5ed8-15b9-46fb-8297-3ec1c8015cfc` |
| L'Impératrice + `Déjà-vue` / L'Imperatrice + `Deja‐vue` | `l'imperatrice` + `deja-vue` | `1ae73055-8fa9-45e9-b937-e4431694e036` |
| Mylène Farmer + `À l'ombre` / `A l'ombre` | `mylene farmer` + `a l'ombre` | `b9ac17f6-f1a6-46f8-990c-76aff60cc468` |
| ROSALÍA + `Yo x ti, tú x mí` / `Yo x Ti, Tu x Mi` | `rosalia` + `yo x ti, tu x mi` | `48e6bbcc-24a6-438b-a697-7e557f4f9326` |
| Róisín Murphy / Roisin Murphy + `Let Me Know` | `roisin murphy` + `let me know` | `1b720ab5-b1c2-4cdc-806f-a709d9799181` |
| Motörhead / Motorhead + `Asylum Choir` | `motorhead` + `asylum choir` | `174d7a8f-ccb3-438d-be36-44d1686f6d6d` |

## 이름을 맞춰도 다른 버전을 포함하는 후보

| 실제 후보 | 공식 녹음 자료에서 확인한 차이 | 판단 |
| --- | --- | --- |
| Björk/Bjork + `Jóga/Joga` | 같은 일반 녹음 후보를 공유하면서 1997-11-29 BBC 라이브도 같은 이름으로 모임 | 표기 비교에 사용하되 그룹 전체 병합 보류 |
| Sigur Rós/Sigur Ros + `Glósóli/Glosoli` | 일반 녹음 후보, 2008년 Alexandra Palace 라이브, Dolby Atmos mix | 버전 구분 유지 |
| Zoé + `Vía Láctea/Via Lactea/Via Láctea` | 일반 녹음 후보와 2010년 MTV Unplugged | 버전 구분 유지 |
| Tiësto/Tiesto + `Adagio for Strings` | original mix와 여러 DJ 믹스에 포함된 녹음 | DJ 믹스 발췌를 흡수하지 않음 |
| José González/Jose Gonzalez + `Heartbeats` | 일반 녹음 후보, Radio 1 Live Lounge, DJ 믹스와 공식 MV 참조 | 참조별 검수 유지 |
| Sigur Rós/Sigur Ros + `Fljótavík/Fljotavik` | Sigur Rós 일반 및 라이브 후보와 VOCES8 녹음 ID가 로컬 이름에 함께 붙음 | 버전 충돌과 아티스트 귀속 불일치를 각각 보류 |

Jóga의 일반 녹음 후보는 `1e97b2aa-6f23-4e16-bd36-777463295715`, BBC 라이브는 `735b3549-eb6d-495b-8ce2-ef0e6ed22071`다. Glósóli는 일반 후보 `af21eaa4-45a5-4868-a924-26042cc9bf78`, 라이브 `9ad1fcd8-a8a9-45ca-b7a4-a520865960d0`, Atmos `cae5bab8-b7c0-47e2-9670-e6f881d9ffe7`다. 해당 차이는 공식 recording의 설명을 다시 읽어 확인했다. 설명이 없는 다른 MBID들을 모두 같은 기본 녹음이나 모두 다른 오디오로 단정하지 않았다.

Fljótavík의 `b5ac249d-fe1b-471f-9436-964b100f7bea`는 공식 녹음 크레딧이 VOCES8(`2a264f51-0a83-4972-858d-a4825ff02ad9`)이다. 로컬 아티스트명은 Sigur Rós이며 공식 Sigur Rós ID `f6f2326f-6b25-4170-b89d-e235b25508e8`와 다르다. 이 불일치는 악센트를 지우기 전 자료에도 존재했다. 새 비교 키가 잘못된 원본 연결까지 함께 끌어오므로, MBID가 채워졌다는 이유만으로 무조건 병합할 수 없다.

새 그룹 사이의 교차 참조 충돌을 찾았다는 것은 새 버전이 생겼다는 뜻이 아니다. 이미 있던 버전들이 악센트 제거 때문에 같은 후보로 연결되는 현상을 확인한 것이다. 실제 제출 오디오의 동일성은 이번 조사에서 검증하지 않았다.

## 기존 충돌과 참조 없는 사례

| 실제 후보 | 확인한 상태 | 처리 |
| --- | --- | --- |
| bôa/Boa + `Duvet` | bôa 철자 안에 일반 후보와 abridged ID가 이미 공존, Boa 철자 쪽은 참조 없음 | 기존 충돌로 분리하고 미확인 행 병합 보류 |
| Motörhead/Motorhead + `Ace of Spades` | Motörhead 철자 안에 라이브와 5.1 믹스 등 참조가 이미 공존, Motorhead 쪽은 참조 없음 | 악센트 제거가 새로 만든 알려진 참조 충돌로 세지 않음 |
| Stromae + `Ta Fête (J.A.C.K Remix)` / `Ta Fete (J.A.C.K Remix)` | 양쪽 녹음 ID 없음 | Remix를 유지하고 동일 녹음 확정 보류 |
| Café del Mar + `Romance Anónimo/Romance Anonimo` | 양쪽 녹음 ID 없음 | 표기 후보만 기록 |

## 아티스트 이름만 같아지는 경우와 전사 제외 대조

실제 bôa의 Duvet (acoustic version) 기록과 Boa의 Shine We Are! 기록을 공식 크레딧으로 대조했다. bôa는 `ca1dcba2-792c-4550-abf2-7b45396e81ca`이고 공식 설명은 UK alternative rock band다. BoA는 `a16d1433-ba89-4f72-a47b-a370add0bb55`이며 South Korean singer and actress로 설명된다. 둘 다 비교용 아티스트명은 boa가 되지만 서로 다른 공식 대상이다. 이는 아티스트 키의 오인 위험을 확인한 대조이며, 같은 곡명까지 겹친 전체 키의 실제 오병합을 관측했다는 뜻은 아니다.

MØ의 Meat on a Stick과 BØRNS의 10,000 Emerald Pools도 실제 대조 행으로 포함했다. 비교용 이름은 각각 mø와 børns다. ø는 이번 악센트 제거로 o가 되지 않으므로 MO나 BORNS와 자동으로 묶지 않는다. Remix 및 Sped Up 구분은 기존 테스트와 규칙을 그대로 유지한다.

## 검증과 재현

선택한 공식 녹음 46개의 제목, 설명과 artist_credit를 원래 MusicBrainz recording 표에서 대조하고 크레딧 17개의 아티스트 ID와 이름 및 설명을 확인했다. 사용한 자료는 기존 로컬 MusicBrainz 스냅샷이며 실시간 최신 카탈로그 전체를 조사한 것은 아니다. Silver 두 표의 로그 지문은 전후 같았고 실제 비교에 쓰인 원래 이름과 MSID 및 관측 MBID는 별도로 보존했다.

새 모듈과 파일이 없을 때 실패를 먼저 확인했다. 추가 테스트 5개는 새 교차 참조 충돌, 기존 충돌의 분리, 같은 참조와 미확인 행 구분, 정준 문자 구성만 다른 행의 제외와 버전 보존, 실제 파일 진입점을 검사한다. 진입점은 출력 덮어쓰기와 오래된 입력을 거부하며, 공식 원본이 준비된 조회값과 달라지면 완료 처리하지 않는 것도 확인한다. 기존 이름과 아티스트 및 Gold 순수 로직을 포함한 관련 테스트 33개가 통과했다. Spark나 운영 집계 재생성은 실행하지 않았다.

```powershell
$env:PYTHONPATH='src'
venv/Scripts/python scripts/investigations/review_accent_expansion.py --prepared data/song-identifiers-20261021 --silver C:/Users/SSAFY/Desktop/jjh/flux-data/monthly-silver-20261019 --output <새표본출력경로>
venv/Scripts/python -m pytest tests/test_accent_review.py tests/test_name_preprocessing_preview.py tests/test_identity_sample_review.py tests/test_name_character_catalog.py tests/test_artist_attribution.py tests/test_monthly_gold.py -q
```

최종 출력은 `data/accent-review-expanded-20261021`이다. pool.parquet는 한정한 아티스트 범위의 표본 풀, before-after.md와 rows.json은 상세 표본 102행의 원문 및 비교 값이다. result.json에는 16개 사례의 이전 키별 참조, 새 충돌 참조 쌍, 공식 녹음과 아티스트 크레딧 및 원본 지문을 보관했다. 문자 키는 후보 생성에만 사용하며 실제 연결 승인은 공식 대상, 버전과 미확인 상태를 함께 검수한다. 전수 적용, Silver와 Gold 변경은 보류한다.
