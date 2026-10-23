# 코드 변경의 자동 검증

2026-10-23에 GitHub Actions의 `데이터 계약 검증`을 추가했다. 데이터 원본 대신 저장소의 작은 제어 입력으로 로직과 실제 엔진의 회귀를 검증한다. 운영 Bronze, Silver와 Gold의 읽기 및 변경과 데이터 업로드는 수행하지 않는다.

## 실행 범위

| 작업 | 환경 | 검증 |
| --- | --- | --- |
| 기본 로직 | Ubuntu 24.04, Python 3.14, requirements-dev.txt | 기본 pytest, 변경 경로별 엔진 선택 |
| dbt 실제 검증 | 별도 Python 3.11, requirements-dbt.txt | 전체와 기간 교체, 삭제 및 종료 복구, 잘못된 설정 거부 |
| GX 실제 검증 | 별도 Python 3.11, requirements-gx.txt | 실제 검사와 문서 생성, 불량과 변조의 결과 전환 차단 |

main의 push와 PR에서 코드 및 의존성 변경을 검사한다. 공통 소스와 테스트 및 스크립트가 바뀌면 두 엔진을 모두 실행한다. dbt 모델만 바뀌면 dbt, GX 의존성만 바뀌면 GX를 선택한다. 변경 범위를 읽을 수 없으면 두 엔진을 모두 실행한다. 문서만 바꾼 push에서는 시작하지 않으며 수동 실행에서는 엔진 검증 포함 여부를 선택할 수 있다. Spark와 전체 데이터의 대조는 별도 명시 실행으로 유지한다.

검사에는 원본 데이터나 서비스 비밀키가 필요하지 않다. 작업 권한은 저장소 읽기이고 checkout의 자격 증명을 남기지 않는다. 공식 checkout 및 setup-python의 v6 커밋을 고정했다. 같은 ref의 이전 실행은 새 실행으로 취소하며 각 작업에는 시간 제한을 둔다. 브랜치 보호의 필수 검사 설정은 아직 적용하지 않았다. 이 CI의 실패와 Gold 결과 전환 차단은 서로 다른 범위다.

## 로컬 재현과 검증 경계

깨끗한 Python 3.14.6 환경에서 개발 의존성만 설치했을 때 PyYAML 누락으로 1개가 실패했다. 기존 개발 환경에는 dbt가 설치한 PyYAML이 있어 드러나지 않았던 문제다. requirements-dev.txt에 같은 6.0.3을 명시한 뒤 969 passed, 18 skipped, 12 deselected를 151.61초에 확인했다. skipped는 설치하지 않은 엔진 및 외부 의존의 범위이고, 실제 dbt와 GX 검사는 각 전용 작업으로 분리한다. 경로 선택 10개는 기본 테스트에 포함된다.

GitHub의 Linux 실행도 [성공했다](measurements/2026-10-23-ci.md). 기본 968 passed와 19 skipped, 실제 dbt 7 passed 및 GX 11 passed를 확인했다. 로컬의 실제 explorer 표본에 의존하는 한 검사는 GitHub에서 제외되므로 Windows의 기본 969개와 구분한다. CI가 실제 대규모 Silver CDF와 Gold를 연결하거나 실제 7일 운영을 입증하는 것은 아니다.

로컬에서는 `python -m pytest -q`로 기본 검사를 실행한다. dbt 환경에서는 워크플로에 지정한 네 테스트 파일을 `-m dbt`로, GX 환경에서는 `tests/test_gx_runtime.py -m gx`로 실행한다. 설치와 실행 명령의 정본은 [.github/workflows/data-contracts.yml](../.github/workflows/data-contracts.yml)이다.

공식 참고 자료: [GitHub의 Python 빌드 및 테스트](https://docs.github.com/en/actions/tutorials/build-and-test-code/python), [checkout](https://github.com/actions/checkout), [setup-python](https://github.com/actions/setup-python).
