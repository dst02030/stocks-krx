CREATE TABLE IF NOT EXISTS krx.idx_bond (
    _ts TIMESTAMPTZ NOT NULL,
    기준일자 DATE NOT NULL,
    지수명 VARCHAR(255) NOT NULL,
    총수익지수_종가 DECIMAL(18, 2),
    총수익지수_대비 DECIMAL(18, 2),
    순가격지수_종가 DECIMAL(18, 2),
    순가격지수_대비 DECIMAL(18, 2),
    제로재투자지수_종가 DECIMAL(18, 2),
    제로재투자지수_대비 DECIMAL(18, 2),
    콜재투자지수_종가 DECIMAL(18, 2),
    콜재투자지수_대비 DECIMAL(18, 2),
    시장가격지수_종가 DECIMAL(18, 2),
    시장가격지수_대비 DECIMAL(18, 2),
    듀레이션 DECIMAL(18, 2),
    컨벡시티 DECIMAL(18, 2),
    ytm DECIMAL(18, 2),
    PRIMARY KEY (기준일자, 지수명)
);