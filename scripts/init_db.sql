CREATE TABLE IF NOT EXISTS instruments (
    symbol       VARCHAR(16) PRIMARY KEY,
    asset_class  VARCHAR(32) NOT NULL,
    currency     CHAR(3) NOT NULL,
    tick_size    NUMERIC(12, 6) NOT NULL,
    lot_size     INTEGER NOT NULL,
    exchange     VARCHAR(16) NOT NULL,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE
);
