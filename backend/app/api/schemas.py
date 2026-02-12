from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class CreateStagingRequest(BaseModel):
    level: Literal["block", "allocation"]
    portfolio_id: str | None = None
    instrument_id: str
    trade_date: date
    settle_date: date | None = None
    quantity: Decimal
    price: Decimal
    quote_currency: str = Field(min_length=3, max_length=3)
    report_currency: str = Field(min_length=3, max_length=3)


class UpdateStagingRequest(BaseModel):
    trade_date: date | None = None
    settle_date: date | None = None
    quantity: Decimal | None = None
    price: Decimal | None = None


class StagingResponse(BaseModel):
    id: str
    status: str
    lifecycle: str
    entry_version: int


class DealAllocationInput(BaseModel):
    portfolio_id: str
    quantity: Decimal


class CreateDealStagingRequest(BaseModel):
    transaction_type: Literal["BUY", "SELL", "BuyEquity", "SellEquity"]
    instrument_id: str
    trade_date: date
    settle_date: date | None = None
    quantity: Decimal
    price: Decimal
    quote_currency: str = Field(min_length=3, max_length=3)
    report_currency: str = Field(min_length=3, max_length=3)
    allocations: list[DealAllocationInput] = Field(min_length=1)
    external_ref: str | None = None


class DealAllocationStagingResponse(BaseModel):
    portfolio_id: str
    quantity: str
    amount_qc: str
    staging_id: str


class CreateDealStagingResponse(BaseModel):
    block_staging_id: str
    deal_block_id: str
    block_amount_qc: str
    allocation_stagings: list[DealAllocationStagingResponse]


class ModifyDealRequest(BaseModel):
    quantity: Decimal
    allocations: list[DealAllocationInput] = Field(min_length=1)


class DealAdjustmentResponse(BaseModel):
    block_staging_id: str
    deal_block_id: str
    block_delta_quantity: str
    block_amount_qc: str
    allocation_stagings: list[DealAllocationStagingResponse]


class InstrumentIdentifierInput(BaseModel):
    id_type_code: str
    id_value: str
    is_primary: bool = False


class InstrumentEquityAttributes(BaseModel):
    shares_outstanding: int | None = None
    free_float_shares: int | None = None
    share_class: str | None = None
    voting_rights: str | None = None
    par_value: Decimal | None = None


class InstrumentFuturesAttributes(BaseModel):
    underlying_instrument_id: str | None = None
    contract_size: Decimal | None = None
    multiplier: Decimal | None = None
    tick_size: Decimal | None = None
    maturity_date: date | None = None
    settlement_type: str | None = None


class InstrumentFxAttributes(BaseModel):
    base_currency: str = Field(min_length=3, max_length=3)
    quote_currency: str = Field(min_length=3, max_length=3)
    settlement_date: date | None = None
    tenor: str | None = None


class InstrumentSwapAttributes(BaseModel):
    underlying_instrument_id: str | None = None
    effective_date: date | None = None
    maturity_date: date | None = None
    notional_currency: str | None = Field(default=None, min_length=3, max_length=3)
    notional_amount: Decimal | None = None
    fixed_rate: Decimal | None = None
    floating_rate_index: str | None = None
    payment_frequency: str | None = None


class InstrumentFixedIncomeAttributes(BaseModel):
    issue_date: date | None = None
    maturity_date: date | None = None
    coupon_rate: Decimal | None = None
    coupon_frequency: str | None = None
    day_count_convention: str | None = None
    face_value: Decimal | None = None
    issuer_name: str | None = None


class CreateInstrumentRequest(BaseModel):
    instrument_type: str
    symbol: str | None = None
    currency: str = Field(min_length=3, max_length=3)
    full_name: str
    short_name: str
    security_type: str
    security_id: str | None = None
    identifiers: list[InstrumentIdentifierInput] = Field(min_length=1)
    equity: InstrumentEquityAttributes | None = None
    futures: InstrumentFuturesAttributes | None = None
    fx: InstrumentFxAttributes | None = None
    swap: InstrumentSwapAttributes | None = None
    fixedincome: InstrumentFixedIncomeAttributes | None = None


class InstrumentResponse(BaseModel):
    instrument_id: str
    instrument_type: str
    symbol: str | None = None
    currency: str
    full_name: str
    short_name: str
    security_type: str
    security_id: str
    default_id_type_code: str
    default_settlement_days: int
    identifiers: list[InstrumentIdentifierInput]
    equity: InstrumentEquityAttributes | None = None
    futures: InstrumentFuturesAttributes | None = None
    fx: InstrumentFxAttributes | None = None
    swap: InstrumentSwapAttributes | None = None
    fixedincome: InstrumentFixedIncomeAttributes | None = None


class UpdateSecurityIdRequest(BaseModel):
    security_id: str
