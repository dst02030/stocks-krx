CREATE TABLE krx.sto_info (
    _ts TIMESTAMPTZ NOT NULL,
    기준일자 DATE NOT NULL,
    표준코드 VARCHAR(12),
    단축코드 VARCHAR(6),
    한글종목명 VARCHAR(64),
    한글종목약명 VARCHAR(20),
    영문종목명 VARCHAR(128),
    상장일 DATE,
    시장구분 VARCHAR(20),
    증권구분 VARCHAR(20),
    소속부 VARCHAR(50),
    주식종류 VARCHAR(20),
    액면가 VARCHAR(20),
    상장주식수 BIGINT,
    PRIMARY KEY (기준일자, 단축코드)
);
