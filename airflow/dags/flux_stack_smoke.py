import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="flux_stack_smoke",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["flux", "smoke"],
)
def flux_stack_smoke():
    @task
    def produce() -> int:
        return 21

    @task
    def double(value: int) -> int:
        return value * 2

    @task
    def check(value: int) -> None:
        if value != 42:
            raise AssertionError(f"기대 42, 실제 {value}")

    check(double(produce()))


flux_stack_smoke()
