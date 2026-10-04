"""덤프 하나에 든 여러 파일을 적재 한 단위로 합친다.

pyspark를 꼭대기에서 import하지 않는다. 스키마 선언과 같은 이유다.
합칠지 말지, 무엇을 다시 매길지는 순수한 판단이고, Spark가 있어야
확인할 수 있는 것은 그 판단의 결과가 아니라 실행뿐이다.
"""


def combine(frames):
    """여러 파일의 데이터프레임을 덤프 하나 분량으로 합친다.

    덤프 하나에 파일이 여럿일 수 있다. 달이 바뀌는 날의 증분 덤프에는
    지난달 파일과 이번 달 파일이 함께 들어 있다.

    파일마다 따로 쓰면 안 된다. replaceWhere가 덤프 단위로 범위를 잡으므로
    두 번째 파일을 쓰는 순간 첫 번째 파일의 행이 통째로 지워진다.
    쓰기는 덤프당 한 번이어야 한다.

    `_record_id`는 다시 매기지 않는다. 파일을 읽을 때 줄 내용의 해시로 이미 붙어
    있고(flux.bronze.ingest.build_dataframe), 내용이 번호를 정하므로 합쳐도 다른 줄끼리
    겹치지 않는다. 예전에는 `monotonically_increasing_id()`로 매겨서 파일마다 따로 매긴
    번호가 겹칠 수 있었고, 그래서 여기서 다시 매겼다. 그 번호는 다시 적재할 때마다
    달라져 백필이 성립하지 않았다.
    """
    frames = list(frames)
    if not frames:
        raise ValueError("합칠 데이터프레임이 없다")

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined
