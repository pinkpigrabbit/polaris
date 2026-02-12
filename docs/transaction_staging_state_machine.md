# 트랜잭션 스테이징 상태머신 + 아이템포턴시 정책

이 문서는 `txn_staging`에 대한 **최소/명확/Temporal-safe** 상태머신을 정의한다.

## 상태(Status)

- `entry`: 사용자 입력/수정 가능(검증 전 작업영역)
- `pre_check`: 기본 검증 통과(필수값, 기본 정합성)
- `position`: 회계 반영 + 포지션 업데이트 완료(IBOR 인트라데이 스타일)
- `allocated`: 블록/알로케이션 정합성 충족
- `settled`: 본 파이프라인 최종 상태(이후 라이프사이클은 별도 프로세스)

`lifecycle`은 `status`와 별개로 관리한다.

- `active` / `cancelled` / `deleted`

## 허용 전이(Allowed Transitions)

이 최소 파이프라인에서는 **전진 전이만 허용**한다.

1. `entry -> pre_check`
2. `pre_check -> position`
3. `position -> allocated`
4. `allocated -> settled`

`settled` 이전까지만 `cancelled/deleted` 처리가 가능하다.

## 수정 가능 규칙(Mutability)

- `txn_staging`는 `status = entry` AND `lifecycle = active`일 때만 수정 가능하다.
- `entry`를 벗어나면 사용자 수정은 거부한다. 정정은 아래 중 하나로 처리한다.
  - 새로운 스테이징 행(`txn_staging.id` 새로 생성)
  - 회계 반영 이후라면 회계는 불변이므로 반대거래(append-only)로 정정

모든 변경은 `txn_staging_change`에 기록해야 한다.

## 아이템포턴시(Idempotency) 정책 (Temporal 재시도 필수)

Temporal Activity는 재시도될 수 있고, API 클라이언트도 재시도할 수 있다. 이를 위해 단일 메커니즘을 사용한다.

- 테이블: `idempotency_record(scope, key)` (UNIQUE)

### API 아이템포턴시

- 클라이언트는 `Idempotency-Key` 헤더 제공을 권장한다.
- Scope 예시:
  - `api:create_staging`
  - `api:process_staging:{staging_id}`
- 동일한 `(scope, key)`가 재호출되면 저장된 `response`를 그대로 반환한다.

### Activity 아이템포턴시

각 Activity는 효과를 만들기 전에 반드시 idempotency key를 선점(claim)해야 한다.

- Scope 예시: `activity:advance_status:{staging_id}`
- Key 예시: `to:{to_status}`

이미 동일 (scope, key)가 존재하면 Activity는 “이미 적용됨”으로 보고 저장된 `response`를 반환한다.

## 상태전이 기록

성공한 상태전이는 반드시 `txn_staging_transition`에 행을 추가해야 한다.

- 유니크 키: `(staging_id, to_status, idempotency_scope, idempotency_key)`

이는 전이에 대한 2차 중복 방지이자 감사(audit) 추적 근거다.

## 동시성 / 안전성

전이는 compare-and-set 형태로 수행한다.

- `UPDATE txn_staging SET status = :to, entry_version = entry_version + 1 ... WHERE id = :id AND status = :from AND lifecycle = 'active'`

`rowcount = 0`이면:

- 현재 상태가 이미 `to_status`면 idempotent success로 처리
- 그 외는 invalid transition으로 거부
