# Flux
ListenBrainz 기반 글로벌 음악 트렌드 분석 및 AI 연계형 데이터 파이프라인

---

## Overview

Flux는 '변화', '흐름'을 의미하는 라틴어 Fluxus에서 유래했습니다.

MetaBrainz의 ListenBrainz에서 발생한 수억 건의 전 세계 음악 청취 로그를 수집·가공하여, 글로벌 음악 소비 동향, 시기별/장르별 트렌드, 아티스트 메타데이터 연계 분석을 수행하는 스트리밍·배치 혼합 데이터 파이프라인입니다.

정제된 데이터 마트는 MongoDB 및 FastAPI를 통해 고속 서빙되며, 추후 AI/LLM 모델이 음악 시장 트렌드 리포트를 자동 생성하거나 추천 시스템의 학습 피처로 즉시 활용할 수 있는 구조를 갖추고 있습니다.

---

## Tech

- Delta Lake 기반 [Medallion Architecture](https://learn.microsoft.com/ko-kr/azure/databricks/lakehouse/medallion) 도입 (Bronze, Silver, Gold 3단계 정제 레이어)
- Kafka 활용. 수억 건 대규모 이벤트 안정적 수집 및 Backpressure 완충
- Spark Structured Streaming 기반 스트리밍 파이프라인으로 무손실 적재
- Spark Batch로 MusicBrainz 메타데이터와의 조인 및 집계
- Data Skew 병목 현상 극복 위해 Salting 기법 적용
- 4단계 성능 실험을 통해 최적화 결과 수치 실측
- 시간에 따라 변경되는 이력 Delta Lake의 SCD Type 2 방식으로 관리
- 가변 중첩 문서 구조를 갖는 분석 마트 MongoDB 이관
- FastAPI 기반 Point Lookup 구현