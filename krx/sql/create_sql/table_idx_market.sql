CREATE TABLE IF NOT EXISTS krx.idx_market (
    _ts TIMESTAMPTZ NOT NULL,
    기준일자 DATE NOT NULL,
    계열구분 VARCHAR(255) NOT NULL,
    지수명 VARCHAR(255) NOT NULL,
    종가 DECIMAL(18, 2),
    대비 DECIMAL(18, 2),
    등락률 DECIMAL(5, 2),
    시가 DECIMAL(18, 2),
    고가 DECIMAL(18, 2),
    저가 DECIMAL(18, 2),
    거래량 BIGINT,
    거래대금 DECIMAL(18, 2),
    상장시가총액 DECIMAL(18, 2),
    PRIMARY KEY (기준일자, 지수명, 계열구분)
);