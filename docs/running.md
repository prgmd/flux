# 무엇을 어떻게 돌리는가

측정 문서 열다섯 개의 재현 명령이 전부 이 형태로 남아 있었다.

```
docker run --rm -m 10g -v <데이터 루트>:/data \
  -e FLUX_STORAGE_ROOT=file:///data ... flux-spark \
  python scripts/gold_dim_artist.py --at 2026-09-15
```

가운데 `...`이 감추고 있던 것이 저장소 마운트와 `PYTHONPATH`다. 그것이 없으면
스크립트가 첫 줄의 `from flux... import`에서 죽는다. **기록된 명령 중 실제로
실행되는 것이 하나도 없었다.**

재현할 수 없는 측정은 증거가 아니라 주장이다. 그래서 명령을 한곳에 적는다.
측정 문서의 `...`은 아래 "Spark 잡"의 마운트 세 줄을 뜻한다.

---

## 준비

### 데이터 루트

저장소 밖의 경로여야 한다. `.env.example`을 `.env`로 복사해 채운다.

```
FLUX_STORAGE_ROOT=file:///D:/flux-data
```

이 아래에 `dumps/`, `staging/`, `bronze/`, `silver/`, `gold/`, `measurements/`가
생긴다. 값은 `file://` URI나 절대 경로, 또는 `s3a://버킷/flux`다.

### 이미지

```bash
docker build -t flux-spark -f docker/spark.Dockerfile .
```

이미지에 `src`, `scripts`, `tests`가 구워지고 `PYTHONPATH=/opt/flux/src`가 박힌다.
그래서 마운트 없이도 돈다. 개발 중에는 마운트로 덮어써서 고친 코드를 바로 쓴다.

---

## Spark 잡

```bash
docker run --rm -m 10g \
  -v /d/flux-data:/data \
  -v "$PWD/src:/opt/flux/src:ro" \
  -v "$PWD/scripts:/opt/flux/scripts:ro" \
  -e FLUX_STORAGE_ROOT=file:///data \
  flux-spark \
  python scripts/gold_fact_listens.py
```

`-m 10g`는 컨테이너 메모리 한도다. 드라이버가 기본 8g를 잡으므로
(`flux/common/spark.py`) 그보다 여유를 둔다.

**`src` 마운트를 빼면 이미지에 구워진 옛 모듈이 쓰인다.** 고친 코드를 돌린다고
믿으면서 다른 코드를 돌리게 되므로, 개발 중에는 반드시 붙인다.

잡은 전부 로컬 모드(`local[*]`)로 돈다. compose의 standalone 클러스터에 붙는 것은
`scripts/cluster_check.py` 하나뿐이다.

### 단계별

```bash
# S1 덤프 수급
python scripts/fetch_incremental_range.py --days 31
python scripts/fetch_musicbrainz.py

# S2 Bronze
python scripts/bronze_ingest.py --dump <덤프 이름>
python scripts/bronze_backfill.py            # 받아 둔 것 전부, 이미 한 것은 건너뜀
python scripts/bronze_verify_raw.py

# S3 Silver
python scripts/silver_build.py
python scripts/extract_mb_tables.py
python scripts/silver_musicbrainz.py

# S4 Gold
python scripts/gold_dim_artist.py --at <YYYY-MM-DD>
python scripts/gold_fact_listens.py
python scripts/gold_rename_drill.py          # 개명 주입 → 이력 확인 → 되돌림
python scripts/gold_pit_join_drill.py        # 시점 조인이 당시 이름을 주는지
```

`--at`은 차원 이력의 `valid_from`이 된다. **같은 `--at`으로 두 번 돌리면서
그 사이에 MusicBrainz 릴리스가 바뀌면 `valid_from == valid_to`인 줄이 생겨
불변식이 깨진다.** 잡이 쓰기 전에 확인하고 `SameDayReload`로 거부한다
(`flux.gold.dim_artist.unclosable`).

`--no-broadcast`를 붙이면 차원 조인에 브로드캐스트를 강제하지 않는다.
S4의 조인 전략 비교에 쓴다.

### 측정 결과

전부 데이터 루트의 `measurements/<이름>.json`에 남는다. 문서로 옮긴 것이
`docs/measurements/`다.

---

## 테스트

### 판단 테스트 — 호스트에서, Spark 없이

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt   # Linux: .venv/bin/python
.venv/Scripts/python -m pytest
```

447개가 3초 안에 돈다. `pytest.ini`의 `pythonpath = src`가 경로를 잡으므로
설치도 필요 없다.

돌아가는 것은 pyspark를 부르지 않는 모듈이다 — 무엇이 바뀌면 새 버전인가,
어느 줄을 남기는가, 이 레코드가 성한가. 판단은 Spark가 필요 없다.

### 번역 테스트 — 컨테이너에서, 작은 Spark로

```bash
docker run --rm flux-spark python -m pytest -m spark
```

기본 실행에서는 제외된다(`pytest.ini`의 `addopts`). 호스트에 JDK 17+가 없으면
Spark 4.x가 뜨지 않기 때문이다. 이미지에는 JDK 21이 들어 있다.

여기서 보는 것은 **판단 모듈과 Spark 잡이 같은 규칙을 쓰는가**다. 이 프로젝트에서
실제로 터진 버그가 전부 그 사이에서 났다 — `_record_id` 16배 부풀림,
`explode`로 9천만 행 소실, 2단계 MERGE의 반쪽 상태. 판단이 틀린 적은 없다.

수십 행짜리 픽스처로 도므로 실제 잡(13분)이 아니라 초 단위로 끝난다.

### 스택 통합 테스트

```bash
docker compose up -d
python -m pytest -m cluster
```

---

## compose 스택

```bash
docker compose up -d                          # Spark, PostgreSQL, Airflow
docker compose --profile tools run --rm spark-client   # 클러스터 연결 확인
docker compose down -v                        # 볼륨까지
```

`.env`에 Airflow 키들이 채워져 있어야 한다. `AIRFLOW_JWT_SECRET`이 컨테이너마다
다르면 태스크가 `Invalid auth token: Signature verification failed`로 실패한다.
