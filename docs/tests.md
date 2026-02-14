# 테스트: 무엇을 증명하는가

이 저장소의 테스트는 **통합(Integration) 우선**이다.

사전 조건(아래가 실행/적용되어 있어야 함):

1. Temporal 서버(사용자가 이미 설치)
2. Polaris 전용 Postgres + Redis (podman compose)
3. DB 스키마 적용: `db/schema.sql`

## 테스트 1: 생성 아이템포턴시 (`backend/tests/test_staging_api.py::test_create_is_idempotent`)

목적:

- `POST /staging-transactions`가 클라이언트 재시도 상황에서도 안전함을 보장한다.

검증 내용:

- 동일한 `Idempotency-Key` 헤더로 요청하면 **동일한** 스테이징 응답을 반환한다.
- 중복 `txn_staging` insert를 방지한다.

동작 방식:

- 동일 JSON + 동일 Idempotency-Key로 POST를 2번 호출
- 응답 JSON이 동일(`id`, `status`, `entry_version`)한지 확인

## 테스트 2: entry 상태에서만 수정 가능 (`backend/tests/test_staging_api.py::test_patch_only_allows_entry`)

목적:

- “staging은 `status=entry`일 때만 수정 가능” 규칙을 강제한다.

검증 내용:

- 행이 `entry`인 동안 PATCH가 성공한다.
- `entry_version`이 증가한다.

## 테스트 3: Temporal 파이프라인 E2E (`backend/tests/test_staging_temporal_pipeline.py::test_process_pipeline_to_settled`)

목적:

- API가 Temporal을 트리거하고, Temporal이 상태를 전진시키며 DB/Redis 부수효과까지 정상 반영되는지 증명한다.

검증 내용:

1. API가 staging row를 생성(`entry`).
2. `/process`로 Temporal workflow 시작.
3. Worker가 Activity를 실행하고 최종 `settled`까지 전진.
4. DB 감사 추적 확인:
   - `txn_staging_transition`에 전이 기록 존재
   - `acct_transaction`이 staging row에 대해 1건 생성
   - `position_current` 업데이트
5. Redis에 `position:{portfolio_id}:{instrument_id}` 키 존재

비고:

- Postgres/Redis/Temporal 접근 불가 또는 스키마 미적용 시, 테스트는 명확한 메시지로 `skip`된다.

## 테스트 4: IBOR NAV (`backend/tests/test_nav_and_ca.py::test_ibor_nav_simple`)

목적:

- 인트라데이 NAV가 live 포지션에서 계산됨을 증명한다.

검증 내용:

- `position_current`를 읽는다.
- 최신 가격(<= now)을 사용한다.
- 동일 통화 자산에 대해 `nav_rc = sum(qty * price)`를 반환한다.

## 테스트 5: ABOR NAV(EOD) (`backend/tests/test_nav_and_ca.py::test_abor_nav_eod_pipeline`)

목적:

- 공식 EOD NAV 파이프라인이 스냅샷 + EOD 마켓데이터를 사용하고, 결과를 ABOR 전용 테이블에 저장함을 증명한다.

검증 내용:

- Temporal workflow가 `position_snapshot_eod`를 생성한다.
- 지정 일자의 `market_price.is_eod = true`를 사용한다.
- `abor_nav_run` + `abor_nav_result`를 저장하고 `/nav/abor/{portfolio_id}/result`로 조회 가능하다.

## 테스트 6: Corporate Action - 현금배당 (`backend/tests/test_nav_and_ca.py::test_ca_cash_dividend_pipeline`)

목적:

- CA 처리가 현금 효과(cash effect)를 생성함을 증명한다.

검증 내용:

- CA 이벤트 생성 + Temporal 처리.
- `cash_dividend`: 현금 상품 포지션 증가(`instrument_type='cash'`, `security_id='CASH_USD'`).
- 포트폴리오 단위 중복 방지는 `ca_effect` 유니크 제약으로 보장.

## 테스트 7: Instrument 확장 API (`backend/tests/test_instrument_api.py`)

목적:

- `instrument` 단일 테이블(`security_id`, `full_name`, `short_name`, `security_type` 포함)과 `instrument_identifier`, subtype 테이블이 API와 함께 정상 동작하는지 검증한다.

검증 내용:

- `POST /instruments`로 주식(stock) 생성 시
  - instrument_type별 기본 식별자 룰(`instrument_type_id_rule`)을 적용한다.
  - 기본 식별자 타입값으로 `security_id`를 자동 설정한다.
  - `instrument_equity` subtype 속성을 저장한다.
- 기본 식별자 타입값이 요청에 없으면 `default_identifier_missing` 오류를 반환한다.
- 선물(futures) 생성 시 `instrument_futures` subtype 속성이 저장/조회된다.
