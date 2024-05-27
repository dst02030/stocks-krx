CREATE TABLE IF NOT EXISTS krx.gen_oil (
    _ts TIMESTAMPTZ NOT NULL,
    기준일자 DATE NOT NULL,
    상장타입 VARCHAR(3) NOT NULL,
    유종구분 VARCHAR(255) NOT NULL,
    가중평균가격경쟁 DECIMAL(9, 2) NOT NULL,
    가중평균가격협의 DECIMAL(9, 2) NOT NULL,
    거래량 BIGINT NOT NULL,
    거래대금 BIGINT NOT NULL,
    PRIMARY KEY (기준일자, 유종구분)
);
