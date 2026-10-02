#!/usr/bin/env bash
#
# 컨테이너에서 Spark 잡을 돌린다. 마운트가 실제로 걸렸는지 확인한 뒤에 돈다.
#
# 왜 래퍼가 필요한가.
#
# 이미지는 src와 scripts를 구워서 들고 있다(docker/spark.Dockerfile). 단독
# 실행을 위한 설계인데 부작용이 있다. **마운트가 실패해도 잡이 그냥 돈다.**
# 없는 모듈을 찾다 죽는 대신 옛 사본을 임포트하므로, 고친 코드를 돌린다고
# 믿으면서 다른 코드를 돌리게 된다.
#
# 실제로 그렇게 됐다. Git Bash의 MSYS가 `-v "$PWD/src:/opt/flux/src:ro"`의
# 오른쪽 `/opt/flux/src`를 윈도우 경로로 바꿔 버려, 마운트는 엉뚱한 자리에
# 걸리고 `/opt/flux/src`는 이미지 사본으로 남았다. 명령줄에 마운트가 적혀
# 있으므로 눈으로는 통과한다.
#
# 그래서 두 가지를 한다.
#   1. MSYS_NO_PATHCONV=1 로 경로 변환을 끈다
#   2. 호스트 소스의 지문을 넘겨, 컨테이너가 임포트할 코드와 대조한다
#
#   ./scripts/spark_run.sh [docker run 옵션...] -- python scripts/무엇.py
set -euo pipefail

export MSYS_NO_PATHCONV=1

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data="${FLUX_DATA_DIR:-}"
if [ -z "$data" ] && [ -f "$repo/.env" ]; then
    data="$(grep -E '^FLUX_DATA_DIR=' "$repo/.env" | cut -d= -f2-)"
fi
[ -n "$data" ] || { echo "FLUX_DATA_DIR가 없다. .env를 보라." >&2; exit 2; }

# sha256sum의 출력 형식이 양쪽에서 다르다. Git Bash 쪽 coreutils는 이진 모드
# 표식을 파일명 앞에 붙이고 데비안 쪽은 공백 두 칸을 쓴다. 내용이 같아도
# 지문이 어긋나므로 해시값만 떼어 쓴다. 파일 목록 자체는 sort로 고정한다.
#
# tests와 pytest.ini도 넣는다. 처음엔 src와 scripts만 덮었는데, 이미지가 tests도
# 구워 들고 있어서 **컨테이너에서 돈 테스트가 옛 파일**이었다. 새로 쓴 테스트
# 세 건이 통과한 줄 알았는데 이미지에는 그 세 건이 아예 없었다.
FINGERPRINT_CMD='{ find src scripts tests -name "*.py" -not -path "*__pycache__*"; echo pytest.ini; } | LC_ALL=C sort | xargs sha256sum | cut -c1-64 | sha256sum | cut -c1-64'

expected="$(cd "$repo" && eval "$FINGERPRINT_CMD")"

opts=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do opts+=("$1"); shift; done
[ "${1:-}" = "--" ] && shift
[ $# -gt 0 ] || { echo "돌릴 명령이 없다. -- 뒤에 적는다." >&2; exit 2; }

quoted=$(printf '%q ' "$@")

exec docker run --rm \
    -v "$data:/data" \
    -v "$repo/src:/opt/flux/src:ro" \
    -v "$repo/scripts:/opt/flux/scripts:ro" \
    -v "$repo/tests:/opt/flux/tests:ro" \
    -v "$repo/pytest.ini:/opt/flux/pytest.ini:ro" \
    -v flux-ivy2:/opt/flux/ivy \
    -e FLUX_STORAGE_ROOT="${FLUX_STORAGE_ROOT:-file:///data}" \
    -e FLUX_SRC_FINGERPRINT="$expected" \
    "${opts[@]}" \
    flux-spark \
    bash -c '
        got=$(cd /opt/flux && '"$FINGERPRINT_CMD"')
        if [ "$got" != "$FLUX_SRC_FINGERPRINT" ]; then
            echo "소스 지문이 다르다. 마운트가 안 걸렸다." >&2
            echo "  호스트   $FLUX_SRC_FINGERPRINT" >&2
            echo "  컨테이너 $got" >&2
            exit 3
        fi
        exec '"$quoted"'
    '
