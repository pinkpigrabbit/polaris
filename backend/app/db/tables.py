from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CHAR,
    Column,
    Date,
    Integer,
    JSON,
    MetaData,
    Numeric,
    Table,
    Text,
    TIMESTAMP,
    text,
)
from sqlalchemy.dialects.postgresql import ENUM, UUID


row_lifecycle_status_enum = ENUM(
    "active",
    "cancelled",
    "deleted",
    name="row_lifecycle_status",
    create_type=False,
)
deal_level_enum = ENUM("block", "allocation", name="deal_level", create_type=False)
pending_trade_status_enum = ENUM(
    "entry",
    "pre_check",
    "position",
    "allocated",
    "settled",
    name="pending_trade_status",
    create_type=False,
)
dr_cr_enum = ENUM("DR", "CR", name="dr_cr", create_type=False)

nav_run_type_enum = ENUM("realtime", "snapshot", "eod", name="nav_run_type", create_type=False)
nav_run_status_enum = ENUM("running", "completed", "failed", name="nav_run_status", create_type=False)

ca_type_enum = ENUM("cash_dividend", "stock_split", name="ca_type", create_type=False)
ca_event_status_enum = ENUM(
    "entry",
    "announced",
    "election_open",
    "processed",
    "cancelled",
    name="ca_event_status",
    create_type=False,
)
ca_election_choice_enum = ENUM("accept", "decline", name="ca_election_choice", create_type=False)


metadata = MetaData()


portfolio = Table(
    "portfolio",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("code", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("report_currency", CHAR(3), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


idempotency_record = Table(
    "idempotency_record",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("scope", Text, nullable=False),
    Column("key", Text, nullable=False),
    Column("request_hash", Text),
    Column("response", JSON),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


pending_trade = Table(
    "pending_trade",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("level", deal_level_enum, nullable=False),
    Column("deal_block_id", BigInteger),
    Column("deal_allocation_id", BigInteger),
    Column("portfolio_id", BigInteger),
    Column("instrument_id", BigInteger, nullable=False),
    Column("trade_date", Date, nullable=False),
    Column("settle_date", Date),
    Column("quantity", Numeric(30, 10), nullable=False),
    Column("price", Numeric(30, 12), nullable=False),
    Column("quote_currency", CHAR(3), nullable=False),
    Column("report_currency", CHAR(3), nullable=False),
    Column("qc_gross_amount", Numeric(30, 12)),
    Column("rc_gross_amount", Numeric(30, 12)),
    Column("qc_tax_amount", Numeric(30, 12)),
    Column("rc_tax_amount", Numeric(30, 12)),
    Column("qc_cost_amount", Numeric(30, 12)),
    Column("rc_cost_amount", Numeric(30, 12)),
    Column("fx_rate_qc_to_rc", Numeric(30, 12)),
    Column("fx_rate_source_id", UUID(as_uuid=True)),
    Column("fx_rate_asof_ts", TIMESTAMP(timezone=True)),
    Column("status", pending_trade_status_enum, nullable=False),
    Column("lifecycle", row_lifecycle_status_enum, nullable=False),
    Column("entry_version", Integer, nullable=False),
    Column("source_system", Text),
    Column("external_id", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


pending_trade_change = Table(
    "pending_trade_change",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("pending_trade_id", BigInteger, key="staging_id", nullable=False),
    Column("changed_at", TIMESTAMP(timezone=True), nullable=False),
    Column("actor", Text),
    Column("change_reason", Text),
    Column("old_row", JSON),
    Column("new_row", JSON),
)


journal_entry = Table(
    "journal_entry",
    metadata,
    Column("id", BigInteger, primary_key=True, server_default=text("next_journal_entry_id(now())")),
    Column("pending_trade_id", BigInteger, key="staging_id"),
    Column("deal_block_id", BigInteger),
    Column("deal_allocation_id", BigInteger),
    Column("effective_date", Date, nullable=False),
    Column("posted_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("trade_type", Text, nullable=False),
    Column("entry_role", Text, nullable=False),
    Column("description", Text),
    Column("reversal_of_entry_id", BigInteger, key="reversal_of_acct_transaction_id"),
    Column("replacement_of_entry_id", BigInteger),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


journal_entry_line = Table(
    "journal_entry_line",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("journal_entry_id", BigInteger, key="acct_transaction_id", nullable=False),
    Column("portfolio_id", BigInteger),
    Column("instrument_id", BigInteger),
    Column("account_code", Text, nullable=False),
    Column("drcr", dr_cr_enum, nullable=False),
    Column("quantity", Numeric(30, 10)),
    Column("amount", Numeric(30, 12), nullable=False),
    Column("currency", CHAR(3), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


position_current = Table(
    "position_current",
    metadata,
    Column("portfolio_id", BigInteger, primary_key=True),
    Column("instrument_id", BigInteger, primary_key=True),
    Column("quantity", Numeric(30, 10), nullable=False),
    Column("cost_basis_rc", Numeric(30, 12)),
    Column("last_journal_entry_id", BigInteger, key="last_acct_transaction_id"),
    Column("version_uuid", UUID(as_uuid=True), nullable=False, server_default=text("gen_random_uuid()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


position_snapshot_eod = Table(
    "position_snapshot_eod",
    metadata,
    Column("asof_date", Date, primary_key=True),
    Column("portfolio_id", BigInteger, primary_key=True),
    Column("instrument_id", BigInteger, primary_key=True),
    Column("quantity", Numeric(30, 10), nullable=False),
    Column("cost_basis_rc", Numeric(30, 12)),
    Column("through_journal_entry_id", BigInteger, key="through_acct_transaction_id"),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
)


market_price = Table(
    "market_price",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("instrument_id", BigInteger, nullable=False),
    Column("asof_date", Date, nullable=False),
    Column("asof_ts", TIMESTAMP(timezone=True), nullable=False),
    Column("price", Numeric(30, 12), nullable=False),
    Column("currency", CHAR(3), nullable=False),
    Column("is_eod", Boolean, nullable=False),
    Column("source_id", UUID(as_uuid=True)),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


instrument = Table(
    "instrument",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("instrument_type", Text, nullable=False),
    Column("symbol", Text),
    Column("name", Text),
    Column("currency", CHAR(3)),
    Column("lifecycle", row_lifecycle_status_enum, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


security_id_type = Table(
    "security_id_type",
    metadata,
    Column("code", Text, primary_key=True),
    Column("description", Text),
    Column("is_system", Boolean, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_type_id_rule = Table(
    "instrument_type_id_rule",
    metadata,
    Column("instrument_type", Text, primary_key=True),
    Column("default_id_type_code", Text, nullable=False),
    Column("updated_by", Text),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_master = Table(
    "instrument_master",
    metadata,
    Column("instrument_id", BigInteger, primary_key=True),
    Column("security_id", Text, nullable=False),
    Column("full_name", Text, nullable=False),
    Column("short_name", Text, nullable=False),
    Column("security_type", Text, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_identifier = Table(
    "instrument_identifier",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("instrument_id", BigInteger, nullable=False),
    Column("id_type_code", Text, nullable=False),
    Column("id_value", Text, nullable=False),
    Column("is_primary", Boolean, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


security_type_rule = Table(
    "security_type_rule",
    metadata,
    Column("security_type", Text, primary_key=True),
    Column("currency", CHAR(3), primary_key=True),
    Column("nav_rule", Text, nullable=False),
    Column("accrued_interest_method", Text, nullable=False),
    Column("price_unit", Text, nullable=False),
    Column("default_settlement_days", Integer, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_equity = Table(
    "instrument_equity",
    metadata,
    Column("instrument_id", BigInteger, primary_key=True),
    Column("shares_outstanding", BigInteger),
    Column("free_float_shares", BigInteger),
    Column("share_class", Text),
    Column("voting_rights", Text),
    Column("par_value", Numeric(30, 12)),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_futures = Table(
    "instrument_futures",
    metadata,
    Column("instrument_id", BigInteger, primary_key=True),
    Column("underlying_instrument_id", BigInteger),
    Column("contract_size", Numeric(30, 12)),
    Column("multiplier", Numeric(30, 12)),
    Column("tick_size", Numeric(30, 12)),
    Column("maturity_date", Date),
    Column("settlement_type", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_fx = Table(
    "instrument_fx",
    metadata,
    Column("instrument_id", BigInteger, primary_key=True),
    Column("base_currency", CHAR(3), nullable=False),
    Column("quote_currency", CHAR(3), nullable=False),
    Column("settlement_date", Date),
    Column("tenor", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_swap = Table(
    "instrument_swap",
    metadata,
    Column("instrument_id", BigInteger, primary_key=True),
    Column("underlying_instrument_id", BigInteger),
    Column("effective_date", Date),
    Column("maturity_date", Date),
    Column("notional_currency", CHAR(3)),
    Column("notional_amount", Numeric(30, 12)),
    Column("fixed_rate", Numeric(30, 12)),
    Column("floating_rate_index", Text),
    Column("payment_frequency", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


instrument_fixedincome = Table(
    "instrument_fixedincome",
    metadata,
    Column("instrument_id", BigInteger, primary_key=True),
    Column("issue_date", Date),
    Column("maturity_date", Date),
    Column("coupon_rate", Numeric(30, 12)),
    Column("coupon_frequency", Text),
    Column("day_count_convention", Text),
    Column("face_value", Numeric(30, 12)),
    Column("issuer_name", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


fx_rate = Table(
    "fx_rate",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("base_currency", CHAR(3), nullable=False),
    Column("quote_currency", CHAR(3), nullable=False),
    Column("asof_ts", TIMESTAMP(timezone=True), nullable=False),
    Column("rate", Numeric(30, 12), nullable=False),
    Column("is_eod", Boolean, nullable=False),
    Column("source_id", UUID(as_uuid=True)),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


ibor_nav_run = Table(
    "ibor_nav_run",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("run_type", nav_run_type_enum, nullable=False),
    Column("portfolio_id", BigInteger, nullable=False),
    Column("asof_ts", TIMESTAMP(timezone=True), nullable=False),
    Column("asof_date", Date, nullable=False),
    Column("report_currency", CHAR(3), nullable=False),
    Column("through_journal_entry_id", BigInteger, key="through_acct_transaction_id"),
    Column("position_version_uuid", UUID(as_uuid=True)),
    Column("status", nav_run_status_enum, nullable=False),
    Column("error_text", Text),
    Column("idempotency_scope", Text),
    Column("idempotency_key", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("completed_at", TIMESTAMP(timezone=True)),
)


ibor_nav_result = Table(
    "ibor_nav_result",
    metadata,
    Column("ibor_nav_run_id", BigInteger, primary_key=True),
    Column("nav_rc", Numeric(30, 12), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


ibor_nav_line_item = Table(
    "ibor_nav_line_item",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("ibor_nav_run_id", BigInteger, nullable=False),
    Column("instrument_id", BigInteger, nullable=False),
    Column("quantity", Numeric(30, 10), nullable=False),
    Column("price", Numeric(30, 12)),
    Column("price_currency", CHAR(3)),
    Column("market_value_rc", Numeric(30, 12), nullable=False),
    Column("fx_rate_to_rc", Numeric(30, 12)),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


abor_nav_run = Table(
    "abor_nav_run",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("run_type", nav_run_type_enum, nullable=False),
    Column("portfolio_id", BigInteger, nullable=False),
    Column("asof_date", Date, nullable=False),
    Column("asof_ts", TIMESTAMP(timezone=True), nullable=False),
    Column("report_currency", CHAR(3), nullable=False),
    Column("position_snapshot_taken_at", TIMESTAMP(timezone=True)),
    Column("through_journal_entry_id", BigInteger, key="through_acct_transaction_id"),
    Column("status", nav_run_status_enum, nullable=False),
    Column("error_text", Text),
    Column("idempotency_scope", Text),
    Column("idempotency_key", Text),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("completed_at", TIMESTAMP(timezone=True)),
)


abor_nav_result = Table(
    "abor_nav_result",
    metadata,
    Column("abor_nav_run_id", BigInteger, primary_key=True),
    Column("nav_rc", Numeric(30, 12), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


abor_nav_line_item = Table(
    "abor_nav_line_item",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("abor_nav_run_id", BigInteger, nullable=False),
    Column("instrument_id", BigInteger, nullable=False),
    Column("quantity", Numeric(30, 10), nullable=False),
    Column("price", Numeric(30, 12)),
    Column("price_currency", CHAR(3)),
    Column("price_asof_ts", TIMESTAMP(timezone=True)),
    Column("price_source_id", UUID(as_uuid=True)),
    Column("market_value_rc", Numeric(30, 12), nullable=False),
    Column("fx_rate_to_rc", Numeric(30, 12)),
    Column("fx_rate_asof_ts", TIMESTAMP(timezone=True)),
    Column("fx_rate_source_id", UUID(as_uuid=True)),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


abor_external_nav = Table(
    "abor_external_nav",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("portfolio_id", BigInteger, nullable=False),
    Column("asof_date", Date, nullable=False),
    Column("nav_rc", Numeric(30, 12), nullable=False),
    Column("report_currency", CHAR(3), nullable=False),
    Column("source_id", UUID(as_uuid=True)),
    Column("received_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


abor_nav_reconcile = Table(
    "abor_nav_reconcile",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("portfolio_id", BigInteger, nullable=False),
    Column("asof_date", Date, nullable=False),
    Column("abor_nav_run_id", BigInteger),
    Column("abor_external_nav_id", BigInteger),
    Column("diff_rc", Numeric(30, 12), nullable=False),
    Column("status", Text, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("resolved_at", TIMESTAMP(timezone=True)),
)


ca_event = Table(
    "ca_event",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("ca_type", ca_type_enum, nullable=False),
    Column("instrument_id", BigInteger, nullable=False),
    Column("ex_date", Date, nullable=False),
    Column("record_date", Date),
    Column("pay_date", Date),
    Column("currency", CHAR(3)),
    Column("cash_amount_per_share", Numeric(30, 12)),
    Column("split_numerator", Numeric(30, 12)),
    Column("split_denominator", Numeric(30, 12)),
    Column("source_id", UUID(as_uuid=True)),
    Column("status", ca_event_status_enum, nullable=False),
    Column("require_election", Boolean, nullable=False),
    Column("lifecycle", row_lifecycle_status_enum, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


ca_election = Table(
    "ca_election",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("ca_event_id", BigInteger, nullable=False),
    Column("portfolio_id", BigInteger, nullable=False),
    Column("choice", ca_election_choice_enum, nullable=False),
    Column("actor", Text),
    Column("elected_at", TIMESTAMP(timezone=True), nullable=False),
)


ca_effect = Table(
    "ca_effect",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("ca_event_id", BigInteger, nullable=False),
    Column("portfolio_id", BigInteger, nullable=False),
    Column("journal_entry_id", BigInteger, key="acct_transaction_id"),
    Column("cash_amount", Numeric(30, 12), nullable=False),
    Column("share_delta", Numeric(30, 10), nullable=False),
    Column("processed_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
)


ca_portfolio_rule = Table(
    "ca_portfolio_rule",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("portfolio_id", BigInteger, nullable=False),
    Column("ca_type", ca_type_enum, nullable=False),
    Column("auto_process", Boolean, nullable=False),
    Column("require_election", Boolean, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)


# Compatibility aliases for existing service code during migration.
txn_staging = pending_trade
txn_staging_change = pending_trade_change
acct_transaction = journal_entry
acct_entry = journal_entry_line
