CREATE TABLE IF NOT EXISTS krx.drv_opt (
    _ts TIMESTAMPTZ NOT NULL,
    기준일자 DATE NOT NULL,
    상품구분 VARCHAR(255) NOT NULL,
    권리유형 VARCHAR(10) NOT NULL,
    종목코드 VARCHAR(10) NOT NULL,
    종목명 VARCHAR(255) NOT NULL,
    상장타입 VARCHAR(20) NOT NULL,
    종가 DECIMAL(9, 2) NOT NULL,
    대비 DECIMAL(9, 2) NOT NULL,
    시가 DECIMAL(9, 2) NOT NULL,
    고가 DECIMAL(9, 2) NOT NULL,
    저가 DECIMAL(9, 2) NOT NULL,
    내재변동성 DECIMAL(9, 2) NOT NULL,
    익일정산가 DECIMAL(9, 2),
    거래량 BIGINT NOT NULL,
    거래대금 BIGINT NOT NULL,
    미결제약정 BIGINT NOT NULL,
    PRIMARY KEY (기준일자, 종목코드, 상품구분)
);

