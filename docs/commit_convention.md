# Git Commit Convention

## 1. Commit Message Format

```text
type: subject
```

* **한 줄로만 작성한다.** body(본문), footer 모두 붙이지 않는다
* subject는 한글로 작성
* 제목 끝에 마침표(`.`) 안 붙임

---

## 2. Type List

| Type | Description |
| --- | --- |
| **feat** | 새로운 기능 추가 |
| **fix** | 버그 수정 |
| **refactor** | 코드 리팩토링 (기능 변경 없음) |
| **docs** | 문서 수정 (README 등) |
| **style** | 코드 포맷팅 등 (코드 로직 변경 없음) |
| **test** | 테스트 코드 추가 또는 수정 |
| **chore** | 빌드 태스크, 패키지 매니저, 환경 설정 수정 |
| **ci** | CI/CD 파이프라인 관련 설정 수정 |

---

## 3. Examples

```text
feat: ListenBrainz 증분 덤프 다운로더 추가
fix: Delta 체크포인트 경로 오류 수정
docs: README 프로젝트 개요 업데이트
chore: docker-compose 환경변수 설정 변경
```

---

## 4. Commit Granularity

* **작은 단위로 쪼개서 커밋한다.** 한 커밋에 여러 관심사를 섞지 않는다
* 커밋 하나가 리뷰 한 번에 읽히는 분량을 넘지 않게 한다
* 브랜치 전략은 **1브랜치 n커밋 1머지**. 커밋마다 브랜치를 새로 파지 않는다

---

## 5. Code Comments

**코드에 주석을 달지 않는다.** 배경과 판단 근거는 코드 밖 문서에 기록한다.
