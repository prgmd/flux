"""탐색 화면의 표본에 넣을 사용자를 고르는 순수 판단. DuckDB를 부르지 않는다.

사용자 몇십 명의 줄을 세 층 전부 뽑아 작은 파일에 담아 두면 화면이 바로 열린다. 어떤 사용자를
담을지가 이 모듈의 일이다. 한 사용자의 모든 줄이 들어가야 중복과 가져오기의 모습이 보이므로
청취가 너무 많은 사용자는 뺀다.
"""

import random
from dataclasses import dataclass

MIN_LISTENS = 1000
MAX_LISTENS = 10000
DEFAULT_SEED = 20261015


@dataclass(frozen=True)
class UserStats:
    user_name: str
    listens: int
    importer_share: float
    clients: int
    removed_duplicates: int
    collab_share: float


KINDS = (
    ("중복 묶음이 있음", 10, lambda u: u.removed_duplicates >= 10),
    ("클라이언트를 여럿 씀", 5, lambda u: u.clients >= 3),
    ("협업곡이 많음", 5, lambda u: u.collab_share >= 0.2),
    ("가져오기 중심", 10, lambda u: u.importer_share >= 0.9),
    ("실시간 앱 중심", 10, lambda u: u.importer_share == 0),
)


def pick_users(stats, *, seed=DEFAULT_SEED, kinds=KINDS, low=MIN_LISTENS, high=MAX_LISTENS):
    """유형마다 정원만큼 사용자를 뽑는다. 한 사용자는 한 유형에만 들어간다.

    입력 순서와 상관없이 같은 씨앗이면 같은 결과가 나온다(이름순으로 정렬한 뒤 섞는다).
    앞 유형이 먼저 뽑으므로 조건이 까다로운 유형을 앞에 둔다.
    """
    pool = sorted((u for u in stats if low <= u.listens <= high), key=lambda u: u.user_name)
    rng = random.Random(seed)
    chosen = {}
    for label, quota, rule in kinds:
        candidates = [u for u in pool if u.user_name not in chosen and rule(u)]
        rng.shuffle(candidates)
        for user in candidates[:quota]:
            chosen[user.user_name] = label
    return chosen


def shortfalls(chosen, kinds=KINDS):
    """정원을 못 채운 유형과 모자란 수."""
    counts = {}
    for label in chosen.values():
        counts[label] = counts.get(label, 0) + 1
    return {label: quota - counts.get(label, 0) for label, quota, _ in kinds if counts.get(label, 0) < quota}
