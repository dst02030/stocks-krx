CREATE TABLE IF NOT EXISTS krx.sto_swcert (
    _ts TIMESTAMPTZ NOT NULL,
    기준일자 DATE NOT NULL,
    시장구분 VARCHAR(20) NOT NULL,
    종목코드 VARCHAR(8) NOT NULL,
    종목명 VARCHAR(255) NOT NULL,
    종가 INTEGER NOT NULL,
    대비 INTEGER NOT NULL,
    등락률 DECIMAL(9, 2) NOT NULL,
    시가 INTEGER NOT NULL,
    고가 INTEGER NOT NULL,
    저가 INTEGER NOT NULL,
    거래량 BIGINT NOT NULL,
    거래대금 BIGINT NOT NULL,
    시가총액 BIGINT NOT NULL,
    상장증서수 INTEGER NOT NULL,
    신주발행가 INTEGER NOT NULL,
    상장폐지일 DATE NOT NULL,
    목적주권_종목코드 VARCHAR(6) NOT NULL,
    목적주권_종목명 VARCHAR(255) NOT NULL,
    목적주권_종가 INTEGER NOT NULL,
    PRIMARY KEY (기준일자, 종목코드)
);