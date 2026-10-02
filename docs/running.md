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
측정 문서의 `...`은 아래 "Spark 잡"의 `scripts/spark_run.sh`가 거는 마운트를 뜻한다.

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

**마운트 없이도 돈다는 것이 함정이다.** 마운트가 실패하면 없는 모듈을 찾다 죽는
대신 이미지 사본을 조용히 임포트한다. 아래 래퍼가 그것을 막는다.

---

## Spark 잡

```bash
./scripts/spark_run.sh -m 10g -- python scripts/gold_fact_listens.py
```

`--` 앞은 `docker run` 옵션, 뒤는 컨테이너에서 돌릴 명령이다. 데이터 루트는
`.env`의 `FLUX_DATA_DIR`에서 읽는다.

`-m 10g`는 컨테이너 메모리 한도다. 드라이버가 기본 8g를 잡으므로
(`flux/common/spark.py`) 그보다 여유를 둔다.

### 왜 `docker run`을 직접 쓰지 않는가

래퍼는 `src`, `scripts`, `tests`, `pytest.ini`를 마운트하고, 호스트와 컨테이너에서
같은 지문을 계산해 **다르면 잡을 돌리지 않는다**(종료코드 3).

직접 쓰던 명령은 이랬다.

```bash
docker run ... -v "$PWD/src:/opt/flux/src:ro" ...
```

Git Bash에서는 이 마운트가 걸리지 않는다. MSYS가 오른쪽의 `/opt/flux/src`를
`C:\Program Files\Git\opt\flux\src`로 바꿔 버린다. 마운트는 엉뚱한 자리에 걸리고
`/opt/flux/src`는 이미지 사본으로 남는다. `/data`는 구성 요소가 하나라 변환을
피해서, 데이터는 제대로 읽히고 코드만 옛것이 된다. 명령줄에 마운트가 적혀 있으니
눈으로는 통과한다.

래퍼는 `MSYS_NO_PATHCONV=1`로 변환을 끄고, 지문 대조로 다른 원인까지 막는다.

### 클러스터에서

compose의 standalone 클러스터(워커 3개, 각 2코어 3GB)에 붙이려면 같은 네트워크에
들어가 마스터를 알려 준다.

```bash
docker compose up -d spark-master spark-worker
./scripts/spark_run.sh -m 6g --network flux_default \
  -v "$PWD/docker/spark-conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro" \
  -e FLUX_SPARK_MASTER=spark://spark-master:7077 \
  -- python scripts/experiment_skew.py --out skew-experiment-cluster
```

컨테이너가 드라이버가 되고 워커가 실행기를 띄운다. 로컬 모드와 다른 점이 둘이다.

- **워커도 데이터를 직접 읽는다.** 드라이버가 읽어서 나눠 주는 것이 아니라 각
  실행기가 자기 몫의 Parquet 파일을 연다. 그래서 compose가 워커에도 데이터 루트를
  마운트한다(`docker-compose.yml`의 `spark-worker`)
- **워커가 드라이버에 되돌아 붙는다.** 셔플 위치와 결과를 주고받으려면 워커가
  드라이버 주소를 알아야 한다. 컨테이너 호스트 이름은 워커가 못 풀 수 있어 IP를
  직접 준다(`flux.common.spark._driver_address`). 로컬 마스터일 때는 넣지 않는다

`--out`을 주지 않으면 로컬 결과(`skew-experiment`)를 덮어쓴다.

`spark-defaults.conf`는 의존 jar 캐시 자리를 고정한다. 빼면 실행할 때마다
delta-spark를 다시 받는다.

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

`--at`은 병합이 여는 새 줄의 `valid_from`이 된다. 처음 만들 때는 `--at`과 무관하게
`1970-01-01`로 쓴다 — 적재일로 쓰면 적재일 이전의 청취가 어떤 차원 줄에도 안 걸린다.

**병합한 날과 같은 `--at`으로 또 병합하면서 그 사이에 MusicBrainz 릴리스가 바뀌면
`valid_from == valid_to`인 줄이 생겨 불변식이 깨진다.** 잡이 쓰기 전에 확인하고
`SameDayReload`로 거부한다(`flux.gold.dim_artist.unclosable`). 처음 만든 직후의
같은 날 병합은 해당하지 않는다. 처음 줄의 `valid_from`이 1970이기 때문이다.

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

554개가 3초 안에 돈다. `pytest.ini`의 `pythonpath = src`가 경로를 잡으므로
설치도 필요 없다.

돌아가는 것은 pyspark를 부르지 않는 모듈이다 — 무엇이 바뀌면 새 버전인가,
어느 줄을 남기는가, 이 레코드가 성한가. 판단은 Spark가 필요 없다.

### 번역 테스트 — 컨테이너에서, 작은 Spark로

```bash
./scripts/spark_run.sh -m 6g -- python -m pytest -m spark -p no:cacheprovider
```

기본 실행에서는 제외된다(`pytest.ini`의 `addopts`). 호스트에 JDK 17+가 없으면
Spark 4.x가 뜨지 않기 때문이다. 이미지에는 JDK 21이 들어 있다.

**래퍼 없이 `docker run --rm flux-spark python -m pytest -m spark`로 돌리면 이미지에
구워진 테스트가 돈다.** 이 문서가 한동안 그 명령을 안내했고, 그 사이 새로 쓴 테스트
세 건이 통과한 줄 알았는데 이미지에는 그 세 건이 없었다. 같은 기간에 테스트 하나가
고친 코드와 어긋나 실패하고 있었는데 그것도 보이지 않았다.

`-p no:cacheprovider`는 읽기 전용으로 마운트한 `tests`에 pytest가 캐시를 쓰려다
경고를 내는 것을 막는다.

여기서 보는 것은 **판단 모듈과 Spark 잡이 같은 규칙을 쓰는가**다. 이 프로젝트에서
실제로 터진 버그가 전부 그 사이에서 났다 — `_record_id` 16배 부풀림,
`explode`로 9,119만 행 소실, 2단계 MERGE의 반쪽 상태. 판단이 틀린 적은 없다.

수십 행짜리 픽스처로 돌지만 **4분 29초 ~ 6분 09초**가 걸린다
(54개, 2026-09-17에 네 번 실측. 같은 입력에 편차가 1분 40초다). 2026-10-02에는
58개가 3분 59초였다(한 번).

예상보다 느리고, 느린 이유가 JVM 기동이 아니다. 세션 setup은 38초로 한 번뿐이고,
나머지는 Delta다 — 차원을 만들고 `MERGE`하는 Gold 테스트가 **행 두세 개에도 각
13~22초**다. Delta 트랜잭션 하나의 고정 비용이 그만큼이고 데이터 크기와 무관하다.

그래서 번역 테스트는 판단 테스트를 대신할 수 없다. 규칙을 판단 모듈에 두고
호스트에서 확인하는 쪽이 여전히 100배 빠르다. 번역 테스트는 **두 층이 갈라졌는지**만
본다.

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
