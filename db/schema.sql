-- Polaris 기본 스키마(최소)
-- - 트랜잭션 스테이징(사용자 입력/수정 가능)
-- - 블록/알로케이션 거래 구조(1 블록 : N 알로케이션)
-- - 회계 원장(불변, 수정/정정은 반대거래로 처리)
-- - 포지션(DB가 원본, Redis는 조회 성능을 위한 캐시)
-- - 상품/포트폴리오/마켓데이터(최소)

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ENUM 타입(상태 코드)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'row_lifecycle_status') THEN
    CREATE TYPE row_lifecycle_status AS ENUM ('active', 'cancelled', 'deleted');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pending_trade_status') THEN
    CREATE TYPE pending_trade_status AS ENUM ('entry', 'pre_check', 'position', 'allocated', 'settled');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'deal_level') THEN
    CREATE TYPE deal_level AS ENUM ('block', 'allocation');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ctm_match_status') THEN
    -- DTCC CTM UI에서 사용하는 매칭 상태 코드 예: NMAT/MISM/MACH/PEND/DISQ/CAND/CCRQ
    CREATE TYPE ctm_match_status AS ENUM ('NMAT', 'MISM', 'MACH', 'PEND', 'DISQ', 'CAND', 'CCRQ');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ctm_match_agreed_status') THEN
    -- Trade-side Match Agreed 상태 코드: NMAG/MAGR/CMAG
    CREATE TYPE ctm_match_agreed_status AS ENUM ('NMAG', 'MAGR', 'CMAG');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dr_cr') THEN
    CREATE TYPE dr_cr AS ENUM ('DR', 'CR');
  END IF;

END$$;

-- 기준(레퍼런스) 테이블
CREATE TABLE IF NOT EXISTS currency (
  code CHAR(3) PRIMARY KEY, -- ISO 4217 통화코드
  name TEXT -- 통화명
);

INSERT INTO currency(code, name)
VALUES ('USD', 'US Dollar')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS data_source (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- PK
  name TEXT NOT NULL UNIQUE -- 데이터 소스명
);

CREATE TABLE IF NOT EXISTS portfolio (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  code TEXT NOT NULL UNIQUE, -- 포트폴리오 코드
  name TEXT NOT NULL, -- 포트폴리오명
  report_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 보고통화(RC)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

CREATE TABLE IF NOT EXISTS instrument (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  instrument_type TEXT NOT NULL, -- 상품 타입(예: stock/bond/fx/cash)
  security_id TEXT NOT NULL, -- 시스템 기준 종목ID(symbol 통합)
  name TEXT, -- 상품명
  full_name TEXT, -- 종목 전체명
  short_name TEXT, -- 종목 약칭
  security_type TEXT, -- 보안유형(level2)
  currency CHAR(3) REFERENCES currency(code), -- 결제/기준 통화(선택)
  lifecycle row_lifecycle_status NOT NULL DEFAULT 'active', -- 활성/취소/삭제 상태
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  UNIQUE (instrument_type, security_id), -- 타입+security_id 중복 방지
  UNIQUE (security_id) -- security_id 전역 중복 방지
);

-- 기존 스키마(symbol + instrument_master)에서 단일 instrument 구조로 마이그레이션
DO $$
BEGIN
  ALTER TABLE instrument ADD COLUMN IF NOT EXISTS security_id TEXT;
  ALTER TABLE instrument ADD COLUMN IF NOT EXISTS full_name TEXT;
  ALTER TABLE instrument ADD COLUMN IF NOT EXISTS short_name TEXT;
  ALTER TABLE instrument ADD COLUMN IF NOT EXISTS security_type TEXT;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'instrument' AND column_name = 'symbol'
  ) THEN
    UPDATE instrument
    SET security_id = symbol
    WHERE security_id IS NULL AND symbol IS NOT NULL;
  END IF;

  IF to_regclass('public.instrument_master') IS NOT NULL THEN
    UPDATE instrument i
    SET
      security_id = COALESCE(i.security_id, m.security_id),
      full_name = COALESCE(i.full_name, m.full_name),
      short_name = COALESCE(i.short_name, m.short_name),
      security_type = COALESCE(i.security_type, m.security_type),
      updated_at = GREATEST(i.updated_at, m.updated_at)
    FROM instrument_master m
    WHERE m.instrument_id = i.id;

    DROP TABLE instrument_master;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'instrument' AND column_name = 'symbol'
  ) THEN
    UPDATE instrument
    SET security_id = symbol
    WHERE security_id IS NULL;
  END IF;

  ALTER TABLE instrument ALTER COLUMN security_id SET NOT NULL;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'instrument' AND column_name = 'symbol'
  ) THEN
    ALTER TABLE instrument DROP COLUMN symbol;
  END IF;

  ALTER TABLE instrument DROP CONSTRAINT IF EXISTS instrument_instrument_type_symbol_key;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'instrument_instrument_type_security_id_key'
  ) THEN
    ALTER TABLE instrument
      ADD CONSTRAINT instrument_instrument_type_security_id_key UNIQUE (instrument_type, security_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'instrument_security_id_key'
  ) THEN
    ALTER TABLE instrument
      ADD CONSTRAINT instrument_security_id_key UNIQUE (security_id);
  END IF;
END$$;

-- 상품 식별자/룰(확장형)
CREATE TABLE IF NOT EXISTS security_id_type (
  code TEXT PRIMARY KEY, -- 식별자 타입 코드(BBG_TICKER/ISIN/CUSIP/BBGID/USER_*)
  description TEXT, -- 식별자 타입 설명
  is_system BOOLEAN NOT NULL DEFAULT TRUE, -- 시스템 기본 타입 여부
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

CREATE TABLE IF NOT EXISTS instrument_type_id_rule (
  instrument_type TEXT PRIMARY KEY, -- 상품 타입(예: stock/fixedincome/abs)
  default_id_type_code TEXT NOT NULL REFERENCES security_id_type(code), -- 해당 타입의 기본 securityId 식별자 타입
  updated_by TEXT, -- 최종 변경자(관리자)
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

CREATE TABLE IF NOT EXISTS security_type_rule (
  security_type TEXT NOT NULL, -- 보안유형(level2, 예: equity_common/corp_bond)
  currency CHAR(3) NOT NULL REFERENCES currency(code), -- 통화
  nav_rule TEXT NOT NULL, -- NAV 계산 규칙 코드
  accrued_interest_method TEXT NOT NULL, -- 경과이자 계산 방법 코드
  price_unit TEXT NOT NULL, -- 가격 단위(예: amount/percent)
  default_settlement_days INTEGER NOT NULL, -- 기본 결제일(T+n)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  PRIMARY KEY (security_type, currency)
);

CREATE TABLE IF NOT EXISTS instrument_identifier (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  instrument_id BIGINT NOT NULL REFERENCES instrument(id) ON DELETE CASCADE, -- 대상 상품
  id_type_code TEXT NOT NULL REFERENCES security_id_type(code), -- 식별자 타입
  id_value TEXT NOT NULL, -- 식별자 값
  is_primary BOOLEAN NOT NULL DEFAULT FALSE, -- 대표 식별자 여부
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  UNIQUE (instrument_id, id_type_code), -- 같은 상품에 같은 식별자 타입 중복 방지
  UNIQUE (id_type_code, id_value) -- 식별자 전역 중복 방지
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_instrument_identifier_primary_one
  ON instrument_identifier (instrument_id)
  WHERE (is_primary);

CREATE TABLE IF NOT EXISTS instrument_equity (
  instrument_id BIGINT PRIMARY KEY REFERENCES instrument(id) ON DELETE CASCADE, -- instrument 기본키(FK)
  shares_outstanding BIGINT, -- 발행주식수
  free_float_shares BIGINT, -- 유통주식수
  share_class TEXT, -- 주식 클래스(보통주/우선주 등)
  voting_rights TEXT, -- 의결권 정보
  par_value NUMERIC(30, 12), -- 액면가
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

CREATE TABLE IF NOT EXISTS instrument_futures (
  instrument_id BIGINT PRIMARY KEY REFERENCES instrument(id) ON DELETE CASCADE, -- instrument 기본키(FK)
  underlying_instrument_id BIGINT REFERENCES instrument(id), -- 기초자산 instrument_id
  contract_size NUMERIC(30, 12), -- 계약 단위
  multiplier NUMERIC(30, 12), -- 승수
  tick_size NUMERIC(30, 12), -- 최소 호가 단위
  maturity_date DATE, -- 만기일
  settlement_type TEXT, -- 결제방식(현금/실물)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

CREATE TABLE IF NOT EXISTS instrument_fx (
  instrument_id BIGINT PRIMARY KEY REFERENCES instrument(id) ON DELETE CASCADE, -- instrument 기본키(FK)
  base_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 기준통화
  quote_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 상대통화
  settlement_date DATE, -- 결제일
  tenor TEXT, -- 만기구분(spot/1M/3M 등)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

CREATE TABLE IF NOT EXISTS instrument_swap (
  instrument_id BIGINT PRIMARY KEY REFERENCES instrument(id) ON DELETE CASCADE, -- instrument 기본키(FK)
  underlying_instrument_id BIGINT REFERENCES instrument(id), -- 기초자산 instrument_id
  effective_date DATE, -- 발효일
  maturity_date DATE, -- 만기일
  notional_currency CHAR(3) REFERENCES currency(code), -- 명목원금 통화
  notional_amount NUMERIC(30, 12), -- 명목원금
  fixed_rate NUMERIC(30, 12), -- 고정금리
  floating_rate_index TEXT, -- 변동금리 지표
  payment_frequency TEXT, -- 이자지급 주기
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

CREATE TABLE IF NOT EXISTS instrument_fixedincome (
  instrument_id BIGINT PRIMARY KEY REFERENCES instrument(id) ON DELETE CASCADE, -- instrument 기본키(FK)
  issue_date DATE, -- 발행일
  maturity_date DATE, -- 만기일
  coupon_rate NUMERIC(30, 12), -- 표면금리
  coupon_frequency TEXT, -- 이표 지급 주기
  day_count_convention TEXT, -- 일수계산 규칙
  face_value NUMERIC(30, 12), -- 액면금액
  issuer_name TEXT, -- 발행기관명
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);

-- 시스템 기본 식별자 타입/룰 시드
INSERT INTO security_id_type(code, description, is_system)
VALUES
  ('BBG_TICKER', 'Bloomberg Ticker(예: AAPL US)', TRUE),
  ('ISIN', '국제증권식별번호', TRUE),
  ('CUSIP', '미국채권/증권 식별코드', TRUE),
  ('BBGID', 'Bloomberg Global ID', TRUE)
ON CONFLICT (code) DO NOTHING;

INSERT INTO instrument_type_id_rule(instrument_type, default_id_type_code, updated_by)
VALUES
  ('stock', 'BBG_TICKER', 'system'),
  ('equity', 'BBG_TICKER', 'system'),
  ('fixedincome', 'ISIN', 'system'),
  ('bond', 'ISIN', 'system'),
  ('abs', 'CUSIP', 'system'),
  ('futures', 'BBG_TICKER', 'system'),
  ('fx', 'BBG_TICKER', 'system'),
  ('swap', 'BBG_TICKER', 'system')
ON CONFLICT (instrument_type) DO NOTHING;

INSERT INTO security_type_rule(
  security_type,
  currency,
  nav_rule,
  accrued_interest_method,
  price_unit,
  default_settlement_days
)
VALUES
  ('equity_common', 'USD', 'EQUITY_MARK_TO_MARKET', 'NONE', 'amount', 2),
  ('futures_index', 'USD', 'FUTURES_MARK_TO_MARKET', 'NONE', 'index_points', 1),
  ('fx_spot', 'USD', 'FX_MARK_TO_MARKET', 'NONE', 'rate', 2),
  ('irs_vanilla', 'USD', 'SWAP_DISCOUNTED_CASHFLOW', 'ACT_360', 'percent', 2),
  ('corp_bond', 'USD', 'FIXED_INCOME_DIRTY_PRICE', '30_360', 'percent', 2)
ON CONFLICT (security_type, currency) DO NOTHING;

-- 마켓데이터(최소; 규모가 커지면 asof_date로 파티셔닝 고려)
CREATE TABLE IF NOT EXISTS market_price (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- PK
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 상품
  asof_date DATE NOT NULL, -- 기준일자
  asof_ts TIMESTAMPTZ NOT NULL, -- 기준시각
  price NUMERIC(30, 12) NOT NULL, -- 가격
  currency CHAR(3) NOT NULL REFERENCES currency(code), -- 가격통화
  is_eod BOOLEAN NOT NULL DEFAULT FALSE, -- EOD 여부(공식 종가 등)
  source_id UUID REFERENCES data_source(id), -- 데이터 소스
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  UNIQUE (instrument_id, asof_ts, source_id) -- 중복 방지
);
CREATE INDEX IF NOT EXISTS idx_market_price_instr_date ON market_price (instrument_id, asof_date);

-- Deal 블록/알로케이션
CREATE TABLE IF NOT EXISTS deal_block (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  external_ref TEXT, -- 외부 참조값(선택)
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 상품
  trade_date DATE NOT NULL, -- 체결일
  settle_date DATE, -- 결제일
  trade_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 거래통화
  quantity NUMERIC(30, 10) NOT NULL, -- 수량(블록)
  price NUMERIC(30, 12) NOT NULL, -- 가격
  match_status ctm_match_status NOT NULL DEFAULT 'NMAT', -- 블록 매칭 상태(CTM)
  match_agreed_status ctm_match_agreed_status NOT NULL DEFAULT 'NMAG', -- 거래측 match agreed 상태(CTM)
  lifecycle row_lifecycle_status NOT NULL DEFAULT 'active', -- 활성/취소/삭제
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);
CREATE INDEX IF NOT EXISTS idx_deal_block_instr_trade_date ON deal_block (instrument_id, trade_date);

CREATE TABLE IF NOT EXISTS deal_allocation (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  block_id BIGINT NOT NULL REFERENCES deal_block(id) ON DELETE RESTRICT, -- 소속 블록
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 배분 포트폴리오
  quantity NUMERIC(30, 10) NOT NULL, -- 수량(alloc)
  price NUMERIC(30, 12) NOT NULL, -- 가격
  is_rounding_adjustment BOOLEAN NOT NULL DEFAULT FALSE, -- 반올림 보정 alloc 여부
  match_status ctm_match_status NOT NULL DEFAULT 'NMAT', -- alloc 매칭 상태(CTM)
  lifecycle row_lifecycle_status NOT NULL DEFAULT 'active', -- 활성/취소/삭제
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 수정시각
);
CREATE INDEX IF NOT EXISTS idx_deal_alloc_block ON deal_allocation (block_id);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_deal_alloc_rounding ON deal_allocation (block_id) WHERE (is_rounding_adjustment);

-- allocation 합계 정합성(수량/금액)은 DB 트리거가 아니라 FastAPI 서비스 로직에서 검증/보정한다.
DROP TRIGGER IF EXISTS trg_validate_allocation_sums ON deal_allocation;
DROP FUNCTION IF EXISTS validate_allocation_sums();

-- 아이템포턴시/중복방지(API 재시도, Temporal activity 재시도 대비)
CREATE TABLE IF NOT EXISTS idempotency_record (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  scope TEXT NOT NULL, -- 키 스코프(예: api:create_staging)
  key TEXT NOT NULL, -- 아이템포턴시 키
  request_hash TEXT, -- 요청 바디 해시(선택)
  response JSONB, -- 저장된 응답(선택)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  UNIQUE (scope, key) -- (scope,key) 유니크
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'idempotency_record'
      AND column_name = 'id'
      AND udt_name = 'uuid'
  ) THEN
    ALTER TABLE idempotency_record DROP CONSTRAINT IF EXISTS idempotency_record_pkey;
    ALTER TABLE idempotency_record RENAME COLUMN id TO id_uuid_legacy;
    ALTER TABLE idempotency_record ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY;
    ALTER TABLE idempotency_record ADD PRIMARY KEY (id);
    ALTER TABLE idempotency_record DROP COLUMN id_uuid_legacy;
  END IF;
END$$;

-- Pending Trade(수정 가능) + 변경 이력
CREATE TABLE IF NOT EXISTS pending_trade (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  level deal_level NOT NULL, -- block/allocation 레벨
  deal_block_id BIGINT REFERENCES deal_block(id), -- 연결된 block(선택)
  deal_allocation_id BIGINT REFERENCES deal_allocation(id), -- 연결된 allocation(선택)
  portfolio_id BIGINT REFERENCES portfolio(id), -- 포트폴리오(alloc일 때 필수)
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 상품
  trade_date DATE NOT NULL, -- 체결일
  settle_date DATE, -- 결제일
  quantity NUMERIC(30, 10) NOT NULL, -- 수량
  price NUMERIC(30, 12) NOT NULL, -- 가격
  quote_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 거래통화(QC)
  report_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 보고통화(RC)

  -- QC/RC 금액을 미리 계산해 저장(조회 성능 및 재현성)
  qc_gross_amount NUMERIC(30, 12), -- 총금액(QC)
  rc_gross_amount NUMERIC(30, 12), -- 총금액(RC)
  qc_tax_amount NUMERIC(30, 12), -- 세금(QC)
  rc_tax_amount NUMERIC(30, 12), -- 세금(RC)
  qc_cost_amount NUMERIC(30, 12), -- 비용(QC)
  rc_cost_amount NUMERIC(30, 12), -- 비용(RC)
  fx_rate_qc_to_rc NUMERIC(30, 12), -- FX(QC->RC)
  fx_rate_source_id UUID REFERENCES data_source(id), -- FX 소스
  fx_rate_asof_ts TIMESTAMPTZ, -- FX 기준시각

  status pending_trade_status NOT NULL DEFAULT 'entry', -- 파이프라인 상태
  lifecycle row_lifecycle_status NOT NULL DEFAULT 'active', -- 활성/취소/삭제
  entry_version INTEGER NOT NULL DEFAULT 1, -- 수정 버전(낙관적 락)
  source_system TEXT, -- 외부 시스템명(선택)
  external_id TEXT, -- 외부 ID(선택)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각

  CHECK (
    (level = 'block' AND deal_allocation_id IS NULL)
    OR (level = 'allocation')
  )
);
CREATE INDEX IF NOT EXISTS idx_pending_trade_status ON pending_trade (status);
CREATE INDEX IF NOT EXISTS idx_pending_trade_portfolio ON pending_trade (portfolio_id);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_pending_trade_source_external
  ON pending_trade (source_system, external_id)
  WHERE (source_system IS NOT NULL AND external_id IS NOT NULL);

CREATE TABLE IF NOT EXISTS pending_trade_change (
  id BIGSERIAL PRIMARY KEY, -- PK
  pending_trade_id BIGINT NOT NULL REFERENCES pending_trade(id) ON DELETE RESTRICT, -- 대상 pending trade
  changed_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 변경시각
  actor TEXT, -- 변경자
  change_reason TEXT, -- 변경사유
  old_row JSONB, -- 변경 전
  new_row JSONB -- 변경 후
);
CREATE INDEX IF NOT EXISTS idx_pending_trade_change_target ON pending_trade_change (pending_trade_id, changed_at);

-- 상태전이 감사는 Temporal workflow history를 기준으로 관리한다.

-- 분개 ID(YYYYMMDD + 일자별 시퀀스)
CREATE TABLE IF NOT EXISTS journal_entry_daily_counter (
  seq_date DATE PRIMARY KEY,
  last_value BIGINT NOT NULL
);

CREATE OR REPLACE FUNCTION next_journal_entry_id(p_created_at TIMESTAMPTZ DEFAULT now())
RETURNS BIGINT AS $$
DECLARE
  v_date DATE := (p_created_at AT TIME ZONE 'UTC')::DATE;
  v_next BIGINT;
BEGIN
  INSERT INTO journal_entry_daily_counter(seq_date, last_value)
  VALUES (v_date, 1)
  ON CONFLICT (seq_date)
  DO UPDATE SET last_value = journal_entry_daily_counter.last_value + 1
  RETURNING last_value INTO v_next;

  RETURN (to_char(v_date, 'YYYYMMDD') || lpad(v_next::TEXT, 9, '0'))::BIGINT;
END;
$$ LANGUAGE plpgsql;

-- 분개 헤더(append-only, 불변)
CREATE TABLE IF NOT EXISTS journal_entry (
  id BIGINT PRIMARY KEY DEFAULT next_journal_entry_id(now()), -- YYYYMMDD000000001
  pending_trade_id BIGINT REFERENCES pending_trade(id), -- 출처 pending trade
  deal_block_id BIGINT REFERENCES deal_block(id), -- 출처 block
  deal_allocation_id BIGINT REFERENCES deal_allocation(id), -- 출처 alloc
  effective_date DATE NOT NULL, -- 효력일
  posted_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 전표시각
  trade_type TEXT NOT NULL, -- BUY/SELL
  entry_role TEXT NOT NULL DEFAULT 'normal', -- normal/reversal/replacement
  description TEXT, -- 설명
  reversal_of_entry_id BIGINT REFERENCES journal_entry(id), -- 반대거래 대상
  replacement_of_entry_id BIGINT REFERENCES journal_entry(id), -- 교체거래 대상
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  CHECK (trade_type IN ('BUY', 'SELL')),
  CHECK (entry_role IN ('normal', 'reversal', 'replacement'))
);
CREATE INDEX IF NOT EXISTS idx_journal_entry_pending_trade ON journal_entry (pending_trade_id);
CREATE INDEX IF NOT EXISTS idx_journal_entry_block_alloc ON journal_entry (deal_block_id, deal_allocation_id, posted_at DESC);

CREATE TABLE IF NOT EXISTS journal_entry_line (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  journal_entry_id BIGINT NOT NULL REFERENCES journal_entry(id) ON DELETE RESTRICT, -- 분개
  portfolio_id BIGINT REFERENCES portfolio(id), -- 포트폴리오(선택)
  instrument_id BIGINT REFERENCES instrument(id), -- 상품(선택)
  account_code TEXT NOT NULL, -- 계정코드
  drcr dr_cr NOT NULL, -- 차/대 구분
  quantity NUMERIC(30, 10), -- 수량(선택)
  amount NUMERIC(30, 12) NOT NULL, -- 금액
  currency CHAR(3) NOT NULL REFERENCES currency(code), -- 통화
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 생성시각
);
CREATE INDEX IF NOT EXISTS idx_journal_entry_line_entry ON journal_entry_line (journal_entry_id);

-- 회계 테이블 불변성 강제(DB 레벨)
CREATE OR REPLACE FUNCTION raise_immutable() RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION '불변 테이블: UPDATE/DELETE는 허용되지 않습니다';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_journal_entry_immutable ON journal_entry;
CREATE TRIGGER trg_journal_entry_immutable
BEFORE UPDATE OR DELETE ON journal_entry
FOR EACH ROW EXECUTE FUNCTION raise_immutable();

DROP TRIGGER IF EXISTS trg_journal_entry_line_immutable ON journal_entry_line;
CREATE TRIGGER trg_journal_entry_line_immutable
BEFORE UPDATE OR DELETE ON journal_entry_line
FOR EACH ROW EXECUTE FUNCTION raise_immutable();

-- 포지션(현재 + EOD 스냅샷)
CREATE TABLE IF NOT EXISTS position_current (
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 상품
  quantity NUMERIC(30, 10) NOT NULL, -- 수량
  cost_basis_rc NUMERIC(30, 12), -- 원가/코스트 베이시스(RC)
  last_journal_entry_id BIGINT REFERENCES journal_entry(id), -- 마지막 반영 전표
  version_uuid UUID NOT NULL DEFAULT gen_random_uuid(), -- 정합성 확인용 버전
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  PRIMARY KEY (portfolio_id, instrument_id)
);

CREATE TABLE IF NOT EXISTS position_snapshot_eod (
  asof_date DATE NOT NULL, -- 기준일자
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 상품
  quantity NUMERIC(30, 10) NOT NULL, -- 수량
  cost_basis_rc NUMERIC(30, 12), -- 원가(RC)
  through_journal_entry_id BIGINT REFERENCES journal_entry(id), -- 어디까지 반영된 전표(선택)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  PRIMARY KEY (asof_date, portfolio_id, instrument_id)
);

-- FX 환율 (최소)
CREATE TABLE IF NOT EXISTS fx_rate (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- PK
  base_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 기준통화(예: USD)
  quote_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 상대통화(예: KRW)
  asof_ts TIMESTAMPTZ NOT NULL, -- 환율 기준시각
  rate NUMERIC(30, 12) NOT NULL, -- base->quote 환율
  is_eod BOOLEAN NOT NULL DEFAULT FALSE, -- EOD(공식) 환율 여부
  source_id UUID REFERENCES data_source(id), -- 데이터 소스
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  UNIQUE (base_currency, quote_currency, asof_ts, source_id) -- 중복방지
);
CREATE INDEX IF NOT EXISTS idx_fx_rate_pair_ts ON fx_rate (base_currency, quote_currency, asof_ts);

-- NAV (IBOR/ABOR)
-- 중요: IBOR/ABOR는 서로 다른 성격의 장부이므로 테이블을 분리한다.
-- - IBOR: 실시간/인트라데이(positions: position_current 기반)
-- - ABOR: 공식/EOD(positions: position_snapshot_eod + EOD price 기반) + 외부 NAV 대사

-- 과거 버전의 통합 테이블(nav_*)이 있으면 제거(신규 설치 기준)
DROP TABLE IF EXISTS nav_reconcile CASCADE;
DROP TABLE IF EXISTS external_nav CASCADE;
DROP TABLE IF EXISTS nav_line_item CASCADE;
DROP TABLE IF EXISTS nav_result CASCADE;
DROP TABLE IF EXISTS nav_run CASCADE;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'nav_valuation_basis') THEN
    DROP TYPE nav_valuation_basis;
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'nav_run_type') THEN
    CREATE TYPE nav_run_type AS ENUM ('realtime', 'snapshot', 'eod');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'nav_run_status') THEN
    CREATE TYPE nav_run_status AS ENUM ('running', 'completed', 'failed');
  END IF;
END$$;

-- IBOR NAV 실행(실시간/스냅샷)
CREATE TABLE IF NOT EXISTS ibor_nav_run (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  run_type nav_run_type NOT NULL, -- 실행종류(realtime/snapshot)
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  asof_ts TIMESTAMPTZ NOT NULL, -- 기준시각(인트라데이)
  asof_date DATE NOT NULL, -- 기준일자(asof_ts의 date)
  report_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 보고통화(RC)
  through_journal_entry_id BIGINT REFERENCES journal_entry(id), -- 어디까지 반영된 회계거래(가능한 경우)
  position_version_uuid UUID, -- position_current 정합성/버전(선택)
  status nav_run_status NOT NULL DEFAULT 'running', -- 상태
  error_text TEXT, -- 실패/오류 메시지
  idempotency_scope TEXT, -- 아이템포턴시 스코프
  idempotency_key TEXT, -- 아이템포턴시 키
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  completed_at TIMESTAMPTZ, -- 완료시각
  CHECK (run_type IN ('realtime', 'snapshot')),
  UNIQUE (portfolio_id, run_type, asof_ts)
);
CREATE INDEX IF NOT EXISTS idx_ibor_nav_run_portfolio_date ON ibor_nav_run (portfolio_id, asof_date);

CREATE TABLE IF NOT EXISTS ibor_nav_result (
  ibor_nav_run_id BIGINT PRIMARY KEY REFERENCES ibor_nav_run(id) ON DELETE RESTRICT, -- NAV 실행 ID
  nav_rc NUMERIC(30, 12) NOT NULL, -- NAV(RC)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 생성시각
);

CREATE TABLE IF NOT EXISTS ibor_nav_line_item (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  ibor_nav_run_id BIGINT NOT NULL REFERENCES ibor_nav_run(id) ON DELETE RESTRICT, -- NAV 실행 ID
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 상품
  quantity NUMERIC(30, 10) NOT NULL, -- 수량
  price NUMERIC(30, 12), -- 가격
  price_currency CHAR(3) REFERENCES currency(code), -- 가격통화
  market_value_rc NUMERIC(30, 12) NOT NULL, -- 평가금액(RC)
  fx_rate_to_rc NUMERIC(30, 12), -- FX(가격통화->RC)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 생성시각
);
CREATE INDEX IF NOT EXISTS idx_ibor_nav_line_item_run ON ibor_nav_line_item (ibor_nav_run_id);

-- ABOR NAV 실행(EOD)
CREATE TABLE IF NOT EXISTS abor_nav_run (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  run_type nav_run_type NOT NULL, -- 실행종류(eod)
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  asof_date DATE NOT NULL, -- 기준일자(EOD)
  asof_ts TIMESTAMPTZ NOT NULL, -- 기준시각(EOD 타임스탬프)
  report_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 보고통화(RC)
  position_snapshot_taken_at TIMESTAMPTZ, -- position_snapshot_eod 스냅샷 생성시각(선택)
  through_journal_entry_id BIGINT REFERENCES journal_entry(id), -- 스냅샷 기준(가능한 경우)
  status nav_run_status NOT NULL DEFAULT 'running', -- 상태
  error_text TEXT, -- 실패/오류 메시지
  idempotency_scope TEXT, -- 아이템포턴시 스코프
  idempotency_key TEXT, -- 아이템포턴시 키
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  completed_at TIMESTAMPTZ, -- 완료시각
  CHECK (run_type IN ('eod')),
  UNIQUE (portfolio_id, run_type, asof_date)
);
CREATE INDEX IF NOT EXISTS idx_abor_nav_run_portfolio_date ON abor_nav_run (portfolio_id, asof_date);

CREATE TABLE IF NOT EXISTS abor_nav_result (
  abor_nav_run_id BIGINT PRIMARY KEY REFERENCES abor_nav_run(id) ON DELETE RESTRICT, -- NAV 실행 ID
  nav_rc NUMERIC(30, 12) NOT NULL, -- NAV(RC)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 생성시각
);

CREATE TABLE IF NOT EXISTS abor_nav_line_item (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  abor_nav_run_id BIGINT NOT NULL REFERENCES abor_nav_run(id) ON DELETE RESTRICT, -- NAV 실행 ID
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 상품
  quantity NUMERIC(30, 10) NOT NULL, -- 수량
  price NUMERIC(30, 12), -- 가격(EOD)
  price_currency CHAR(3) REFERENCES currency(code), -- 가격통화
  price_asof_ts TIMESTAMPTZ, -- 가격 기준시각(EOD price timestamp)
  price_source_id UUID REFERENCES data_source(id), -- 가격 소스
  market_value_rc NUMERIC(30, 12) NOT NULL, -- 평가금액(RC)
  fx_rate_to_rc NUMERIC(30, 12), -- FX(가격통화->RC)
  fx_rate_asof_ts TIMESTAMPTZ, -- FX 기준시각
  fx_rate_source_id UUID REFERENCES data_source(id), -- FX 소스
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 생성시각
);
CREATE INDEX IF NOT EXISTS idx_abor_nav_line_item_run ON abor_nav_line_item (abor_nav_run_id);

-- ABOR 외부 NAV(수탁/외부 시스템) 수신
CREATE TABLE IF NOT EXISTS abor_external_nav (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  asof_date DATE NOT NULL, -- 기준일자
  nav_rc NUMERIC(30, 12) NOT NULL, -- 외부 NAV(RC)
  report_currency CHAR(3) NOT NULL REFERENCES currency(code), -- 보고통화(RC)
  source_id UUID REFERENCES data_source(id), -- 소스
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수신시각
  UNIQUE (portfolio_id, asof_date, source_id)
);

-- ABOR NAV 대사
CREATE TABLE IF NOT EXISTS abor_nav_reconcile (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  asof_date DATE NOT NULL, -- 기준일자
  abor_nav_run_id BIGINT REFERENCES abor_nav_run(id), -- 내부 ABOR NAV 실행
  abor_external_nav_id BIGINT REFERENCES abor_external_nav(id), -- 외부 NAV
  diff_rc NUMERIC(30, 12) NOT NULL, -- 차이(RC)
  status TEXT NOT NULL DEFAULT 'open', -- 상태(open/resolved 등)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  resolved_at TIMESTAMPTZ -- 해결시각
);

-- Corporate Action(기업행위, CA)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ca_type') THEN
    CREATE TYPE ca_type AS ENUM ('cash_dividend', 'stock_split');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ca_event_status') THEN
    CREATE TYPE ca_event_status AS ENUM ('entry', 'announced', 'election_open', 'processed', 'cancelled');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ca_election_choice') THEN
    CREATE TYPE ca_election_choice AS ENUM ('accept', 'decline');
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS ca_portfolio_rule (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  ca_type ca_type NOT NULL, -- CA 타입
  auto_process BOOLEAN NOT NULL DEFAULT TRUE, -- 자동 처리 여부
  require_election BOOLEAN NOT NULL DEFAULT FALSE, -- 사용자 선택 필요 여부
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  UNIQUE (portfolio_id, ca_type) -- 포트폴리오+타입 유니크
);

CREATE TABLE IF NOT EXISTS ca_event (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  ca_type ca_type NOT NULL, -- CA 타입
  instrument_id BIGINT NOT NULL REFERENCES instrument(id), -- 대상 상품
  ex_date DATE NOT NULL, -- 권리락일(ex-date)
  record_date DATE, -- 기준일(record date)
  pay_date DATE, -- 지급일(pay date)
  currency CHAR(3) REFERENCES currency(code), -- 현금 CA 통화
  cash_amount_per_share NUMERIC(30, 12), -- 현금배당(주당)
  split_numerator NUMERIC(30, 12), -- 액면분할 분자
  split_denominator NUMERIC(30, 12), -- 액면분할 분모
  source_id UUID REFERENCES data_source(id), -- 데이터 소스
  status ca_event_status NOT NULL DEFAULT 'entry', -- 상태
  require_election BOOLEAN NOT NULL DEFAULT FALSE, -- 선택 필요 여부
  lifecycle row_lifecycle_status NOT NULL DEFAULT 'active', -- 활성/취소/삭제
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 생성시각
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 수정시각
  CHECK (
    (ca_type = 'cash_dividend' AND cash_amount_per_share IS NOT NULL)
    OR (ca_type = 'stock_split' AND split_numerator IS NOT NULL AND split_denominator IS NOT NULL)
  )
);
CREATE INDEX IF NOT EXISTS idx_ca_event_instr_date ON ca_event (instrument_id, ex_date);

CREATE TABLE IF NOT EXISTS ca_election (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  ca_event_id BIGINT NOT NULL REFERENCES ca_event(id) ON DELETE RESTRICT, -- CA 이벤트
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  choice ca_election_choice NOT NULL, -- 선택(수락/거절)
  actor TEXT, -- 선택자
  elected_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 선택시각
  UNIQUE (ca_event_id, portfolio_id) -- 이벤트+포트폴리오 유니크
);

CREATE TABLE IF NOT EXISTS ca_effect (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- PK(정렬 가능한 순번 ID)
  ca_event_id BIGINT NOT NULL REFERENCES ca_event(id) ON DELETE RESTRICT, -- CA 이벤트
  portfolio_id BIGINT NOT NULL REFERENCES portfolio(id), -- 포트폴리오
  journal_entry_id BIGINT REFERENCES journal_entry(id), -- 생성된 회계 전표(선택)
  cash_amount NUMERIC(30, 12) NOT NULL DEFAULT 0, -- 현금 효과
  share_delta NUMERIC(30, 10) NOT NULL DEFAULT 0, -- 주식 수량 변화
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 처리시각
  UNIQUE (ca_event_id, portfolio_id) -- 이벤트+포트폴리오 유니크
);

COMMIT;
