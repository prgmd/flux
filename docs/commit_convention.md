# Git Commit Convention

## 1. Commit Message Format

```text
type: subject
```

* subject는 한글로 작성
* body(본문) 생략
* 제목 끝에 마침표(`.`) 안 붙임

---

## 2. Type List

| Type | Description |
| --- | --- |
| **feat** | 새로운 기능 추가 |
| **fix** | 버그 수정 |
| **refactor** | 코드 리팩토링 (기능 변경 없음) |
| **docs** | 문서 수정 (README, 주석 등) |
| **style** | 코드 포맷팅, 세미콜론 누락 등 (코드 로직 변경 없음) |
| **test** | 테스트 코드 추가 또는 수정 |
| **chore** | 빌드 태스크, 패키지 매니저, 환경 설정 수정 |
| **ci** | CI/CD 파이프라인 관련 설정 수정 |

---

## 3. Examples

```text
feat: Kafka Replayer 배속 재생 기능 추가
fix: Spark Streaming Checkpoint 유실 오류 수정
docs: README 프로젝트 개요 업데이트
chore: docker-compose 환경변수 설정 변경
```