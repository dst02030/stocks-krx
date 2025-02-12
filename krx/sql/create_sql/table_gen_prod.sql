CREATE TABLE IF NOT EXISTS krx.gen_prod (
    _ts TIMESTAMPTZ NOT NULL,
    기준일자 DATE NOT NULL,
    종목코드 VARCHAR(10) NOT NULL,
    상장타입 VARCHAR(4) NOT NULL,
    종목명 VARCHAR(255) NOT NULL,
    종가 DECIMAL(18, 2) NOT NULL,
    대비 DECIMAL(18, 2) NOT NULL,
    등락률 DECIMAL(5, 2) NOT NULL,
    시가 DECIMAL(18, 2) NOT NULL,
    고가 DECIMAL(18, 2) NOT NULL,
    저가 DECIMAL(18, 2) NOT NULL,
    거래량 BIGINT NOT NULL,
    거래대금 BIGINT NOT NULL,
    PRIMARY KEY (기준일자, 종목코드)
);
