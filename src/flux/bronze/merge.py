"""덤프 하나에 든 여러 파일을 적재 한 단위로 합친다.

pyspark를 꼭대기에서 import하지 않는다. 스키마 선언과 같은 이유다.
합칠지 말지, 무엇을 다시 매길지는 순수한 판단이고, Spark가 있어야
확인할 수 있는 것은 그 판단의 결과가 아니라 실행뿐이다.
"""


def _default_record_id():
    from pyspark.sql import functions as F

    return F.monotonically_increasing_id()


def combine(frames, *, record_id=None):
    """여러 파일의 데이터프레임을 덤프 하나 분량으로 합친다.

    덤프 하나에 파일이 여럿일 수 있다. 달이 바뀌는 날의 증분 덤프에는
    지난달 파일과 이번 달 파일이 함께 들어 있다.

    파일마다 따로 쓰면 안 된다. replaceWhere가 덤프 단위로 범위를 잡으므로
    두 번째 파일을 쓰는 순간 첫 번째 파일의 행이 통째로 지워진다.
    쓰기는 덤프당 한 번이어야 한다.

    `_record_id`도 여기서 다시 매긴다. 파일마다 따로 매긴 번호는
    합치면 겹칠 수 있고, 그러면 "적재 안에서 유일"이라는 약속이 깨진다.
    """
    frames = list(frames)
    if not frames:
        raise ValueError("합칠 데이터프레임이 없다")

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)

    if record_id is None:
        record_id = _default_record_id()
    return combined.withColumn("_record_id", record_id)
