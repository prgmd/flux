"""셔플 출력이 워커 디스크에 남은 순간에 워커 하나를 죽인다. 호스트에서 돈다.

    PYTHONPATH=src python scripts/kill_worker_mid_shuffle.py --container flux-spark-worker-2

experiment_worker_kill.py --phase killed를 띄운 뒤 다른 창에서 돌린다. 그 잡의
드라이버가 4040으로 UI를 열어 두므로, 단계 상태를 보다가 첫 셔플 단계가 끝나고
다음 단계가 도는 순간에 `docker kill`을 부른다(flux.common.sparkui.ready_to_kill).

**사람이 손으로 죽이면 시점이 매번 다르다.** 너무 이르면 잃을 출력이 없고
너무 늦으면 다시 할 일이 없다. 둘 다 결과는 같게 나오므로 판정이 가짜가 된다.

표준 라이브러리만 쓴다. 호스트에는 pyspark가 없다.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from flux.common import sparkui


def get(url: str):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="flux-spark-worker-2")
    parser.add_argument("--ui", default="http://localhost:4040")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--out", default=None,
                        help="죽인 순간의 단계 상태를 남길 JSON 경로")
    args = parser.parse_args()

    base = f"{args.ui}/api/v1/applications"
    deadline = time.time() + args.timeout
    app = None
    while time.time() < deadline:
        try:
            apps = get(base)
            if apps:
                app = apps[0]["id"]
                break
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(2)
    if app is None:
        print("UI가 열리지 않았다. 잡이 떴는지, -p 4040:4040을 줬는지 본다")
        return 1
    print(f"앱 {app}")

    while time.time() < deadline:
        try:
            stages = get(f"{base}/{app}/stages")
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            print("UI가 닫혔다. 죽이기 전에 잡이 끝났다")
            return 1
        if sparkui.ready_to_kill(stages):
            executors = get(f"{base}/{app}/executors")
            snapshot = {
                "app": app,
                "container": args.container,
                "killed_at": time.time(),
                "finished_shuffle_maps": sparkui.finished_shuffle_maps(stages),
                "running": sparkui.running(stages),
                "executors": [
                    {"id": e["id"], "hostPort": e.get("hostPort"),
                     "completedTasks": e.get("completedTasks"),
                     "activeTasks": e.get("activeTasks")}
                    for e in executors if e["id"] != "driver"],
            }
            subprocess.run(["docker", "kill", args.container], check=True)
            print(f"죽였다 {args.container}")
            print(f"  끝난 셔플 단계 {snapshot['finished_shuffle_maps']}")
            print(f"  도는 단계      {snapshot['running']}")
            for e in snapshot["executors"]:
                print(f"  실행기 {e['id']:>3} {e['hostPort']:<22} "
                      f"끝낸 {e['completedTasks']:>5}  도는 {e['activeTasks']}")
            if args.out:
                Path(args.out).write_text(
                    json.dumps(snapshot, ensure_ascii=False, indent=2),
                    encoding="utf-8")
            return 0
        time.sleep(2)

    print("시간 안에 죽일 순간이 오지 않았다")
    return 1


if __name__ == "__main__":
    sys.exit(main())
