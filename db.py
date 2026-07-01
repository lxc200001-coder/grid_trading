"""
DuckDB 数据库层 — 网格策略数据持久化
====================================
存储: 策略配置、订单记录、成交明细、行情快照、权益曲线
"""

import duckdb
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS seq_grid_configs;
CREATE SEQUENCE IF NOT EXISTS seq_orders;
CREATE SEQUENCE IF NOT EXISTS seq_fills;
CREATE SEQUENCE IF NOT EXISTS seq_closed_trades;

-- ========== 1. 网格策略配置 ==========
CREATE TABLE IF NOT EXISTS grid_configs (
    id              INTEGER DEFAULT nextval('seq_grid_configs') PRIMARY KEY,
    name            VARCHAR NOT NULL,
    symbol          VARCHAR NOT NULL,
    exchange        VARCHAR DEFAULT 'SMART',
    currency        VARCHAR DEFAULT 'USD',
    sec_type        VARCHAR DEFAULT 'STK',
    lower_price     DOUBLE NOT NULL,
    upper_price     DOUBLE NOT NULL,
    grid_levels     INTEGER NOT NULL,
    qty_per_level   INTEGER NOT NULL,
    max_position    INTEGER DEFAULT 0,
    status          VARCHAR DEFAULT 'ACTIVE',    -- ACTIVE | PAUSED | STOPPED
    dry_run         BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT now(),
    updated_at      TIMESTAMP DEFAULT now()
);

-- ========== 2. 订单记录 ==========
CREATE TABLE IF NOT EXISTS orders (
    id              BIGINT DEFAULT nextval('seq_orders') PRIMARY KEY,
    grid_config_id  INTEGER REFERENCES grid_configs(id),
    grid_level      INTEGER,                    -- 对应第几层网格
    ib_order_id     BIGINT,                     -- IBKR 订单号
    action          VARCHAR NOT NULL,            -- BUY / SELL
    order_type      VARCHAR DEFAULT 'LMT',
    price           DOUBLE NOT NULL,
    quantity        INTEGER NOT NULL,
    filled_qty      INTEGER DEFAULT 0,
    avg_fill_price  DOUBLE,
    status          VARCHAR DEFAULT 'SUBMITTED', -- SUBMITTED | FILLED | CANCELLED | EXPIRED
    message         VARCHAR,
    created_at      TIMESTAMP DEFAULT now(),
    filled_at       TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT now()
);

-- ========== 3. 成交明细 (每个 fill 一条) ==========
CREATE TABLE IF NOT EXISTS fills (
    id              BIGINT DEFAULT nextval('seq_fills') PRIMARY KEY,
    order_id        BIGINT REFERENCES orders(id),
    ib_exec_id      VARCHAR UNIQUE,              -- IBKR execution ID (去重)
    action          VARCHAR,
    fill_price      DOUBLE NOT NULL,
    fill_qty        INTEGER NOT NULL,
    fill_time       TIMESTAMP NOT NULL,
    commission      DOUBLE DEFAULT 0,
    recorded_at     TIMESTAMP DEFAULT now()
);

-- ========== 4. 已平仓交易对 (配对盈亏) ==========
CREATE TABLE IF NOT EXISTS closed_trades (
    id              BIGINT DEFAULT nextval('seq_closed_trades') PRIMARY KEY,
    grid_config_id  INTEGER REFERENCES grid_configs(id),
    buy_order_id    BIGINT REFERENCES orders(id),
    sell_order_id   BIGINT REFERENCES orders(id),
    symbol          VARCHAR,
    buy_price       DOUBLE,
    sell_price      DOUBLE,
    quantity        INTEGER,
    gross_pnl       DOUBLE,
    net_pnl         DOUBLE,
    roi_pct         DOUBLE,
    open_time       TIMESTAMP,
    close_time      TIMESTAMP,
    hold_seconds    BIGINT
);

-- ========== 5. 行情快照 (定时或 tick 级) ==========
CREATE TABLE IF NOT EXISTS market_snapshots (
    symbol          VARCHAR NOT NULL,
    price           DOUBLE,
    bid             DOUBLE,
    ask             DOUBLE,
    volume          BIGINT,
    timestamp       TIMESTAMP NOT NULL
);

-- ========== 6. 权益曲线 (定时记录) ==========
CREATE TABLE IF NOT EXISTS equity_curve (
    timestamp       TIMESTAMP NOT NULL,
    total_value     DOUBLE,
    cash            DOUBLE,
    position_value  DOUBLE,
    unrealized_pnl  DOUBLE,
    realized_pnl    DOUBLE,
    margin_used     DOUBLE
);

-- ========== 索引 ==========
CREATE INDEX IF NOT EXISTS idx_orders_config   ON orders(grid_config_id);
CREATE INDEX IF NOT EXISTS idx_orders_status   ON orders(status);
CREATE INDEX IF NOT EXISTS idx_fills_order     ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_fills_time      ON fills(fill_time);
CREATE INDEX IF NOT EXISTS idx_market_sym_time ON market_snapshots(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_equity_time     ON equity_curve(timestamp);
"""


class Database:
    """DuckDB 数据库封装"""

    def __init__(self, db_path: str | Path = "grid_trading.duckdb"):
        self.db_path = Path(db_path)
        self.conn: duckdb.DuckDBPyConnection | None = None

    # ---- 连接 & 初始化 ----

    def connect(self) -> "Database":
        """连接数据库并初始化 schema"""
        self.conn = duckdb.connect(str(self.db_path))
        try:
            self.conn.execute("INSTALL parquet")
        except Exception:
            pass  # 已安装
        try:
            self.conn.execute("LOAD parquet")
        except Exception:
            pass
        self.conn.execute(SCHEMA_SQL)
        logger.info(f"✅ 数据库已连接: {self.db_path.absolute()}")
        return self

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("🔌 数据库连接已关闭")

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        self.close()

    # ---- 配置管理 ----

    def save_config(self, cfg: dict) -> int:
        """保存策略配置，返回 config_id"""
        row = self.conn.execute("""
            INSERT INTO grid_configs (
                name, symbol, exchange, currency, sec_type,
                lower_price, upper_price, grid_levels, qty_per_level,
                max_position, status, dry_run
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, [
            cfg.get("name", f"{cfg['symbol']}_grid"),
            cfg["symbol"], cfg.get("exchange", "SMART"),
            cfg.get("currency", "USD"), cfg.get("sec_type", "STK"),
            cfg["lower_price"], cfg["upper_price"],
            cfg["grid_levels"], cfg["qty_per_level"],
            cfg.get("max_position", 0),
            cfg.get("status", "ACTIVE"),
            cfg.get("dry_run", True),
        ]).fetchone()[0]
        logger.info(f"💾 策略配置已保存 (id={row})")
        return row

    def update_config_status(self, config_id: int, status: str):
        self.conn.execute("""
            UPDATE grid_configs SET status = ?, updated_at = now()
            WHERE id = ?
        """, [status, config_id])

    def list_configs(self) -> list[dict]:
        return self.conn.execute("""
            SELECT * FROM grid_configs ORDER BY created_at DESC
        """).fetchdf().to_dict("records")

    # ---- 订单记录 ----

    def save_order(self, order: dict) -> int:
        """保存订单，返回自增 id"""
        row = self.conn.execute("""
            INSERT INTO orders (
                grid_config_id, grid_level, ib_order_id,
                action, order_type, price, quantity,
                status, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, [
            order["grid_config_id"], order.get("grid_level"),
            order.get("ib_order_id"), order["action"],
            order.get("order_type", "LMT"), order["price"],
            order["quantity"], order.get("status", "SUBMITTED"),
            order.get("message"),
        ]).fetchone()[0]
        return row

    def update_order(self, order_id: int, updates: dict):
        """更新订单状态（成交、取消等）"""
        sets = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [order_id]
        self.conn.execute(f"UPDATE orders SET {sets}, updated_at = now() WHERE id = ?", vals)

    def get_active_orders(self, config_id: int) -> list[dict]:
        return self.conn.execute("""
            SELECT * FROM orders
            WHERE grid_config_id = ? AND status IN ('SUBMITTED', 'PENDING')
            ORDER BY price
        """, [config_id]).fetchdf().to_dict("records")

    # ---- 成交记录 ----

    def save_fill(self, fill: dict) -> int:
        """保存成交明细，去重"""
        existing = self.conn.execute(
            "SELECT id FROM fills WHERE ib_exec_id = ?",
            [fill["ib_exec_id"]]
        ).fetchone()
        if existing:
            return existing[0]
        row = self.conn.execute("""
            INSERT INTO fills (order_id, ib_exec_id, action, fill_price, fill_qty, fill_time, commission)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, [
            fill["order_id"], fill["ib_exec_id"], fill["action"],
            fill["fill_price"], fill["fill_qty"],
            fill["fill_time"], fill.get("commission", 0),
        ]).fetchone()[0]
        return row

    # ---- 盈亏配对 ----

    def close_trade(self, trade: dict) -> int:
        """记录已平仓交易对"""
        row = self.conn.execute("""
            INSERT INTO closed_trades (
                grid_config_id, buy_order_id, sell_order_id,
                symbol, buy_price, sell_price, quantity,
                gross_pnl, net_pnl, roi_pct,
                open_time, close_time, hold_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, [
            trade["grid_config_id"], trade["buy_order_id"],
            trade["sell_order_id"], trade["symbol"],
            trade["buy_price"], trade["sell_price"],
            trade["quantity"], trade["gross_pnl"],
            trade["net_pnl"], trade["roi_pct"],
            trade["open_time"], trade["close_time"],
            trade["hold_seconds"],
        ]).fetchone()[0]
        return row

    # ---- 行情快照 ----

    def save_market_snapshot(self, symbol: str, price: float,
                              bid: float, ask: float, volume: int):
        self.conn.execute("""
            INSERT INTO market_snapshots (symbol, price, bid, ask, volume, timestamp)
            VALUES (?, ?, ?, ?, ?, now())
        """, [symbol, price, bid, ask, volume])

    def save_market_snapshots_batch(self, rows: list[tuple]):
        """批量写入行情数据"""
        self.conn.executemany("""
            INSERT INTO market_snapshots (symbol, price, bid, ask, volume, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, rows)

    # ---- 权益曲线 ----

    def save_equity_snapshot(self, data: dict):
        self.conn.execute("""
            INSERT INTO equity_curve (timestamp, total_value, cash, position_value,
                                      unrealized_pnl, realized_pnl, margin_used)
            VALUES (now(), ?, ?, ?, ?, ?, ?)
        """, [
            data["total_value"], data["cash"], data["position_value"],
            data.get("unrealized_pnl", 0), data.get("realized_pnl", 0),
            data.get("margin_used", 0),
        ])

    # ---- 分析查询 ----

    def get_pnl_summary(self, config_id: int) -> dict:
        """统计盈亏汇总"""
        rows = self.conn.execute("""
            SELECT
                COUNT(*)           AS total_trades,
                COUNT(*) FILTER(WHERE net_pnl > 0) AS winning_trades,
                COALESCE(SUM(net_pnl), 0)           AS total_net_pnl,
                COALESCE(AVG(roi_pct), 0)           AS avg_roi_pct,
                COALESCE(SUM(hold_seconds), 0)       AS total_hold_seconds
            FROM closed_trades
            WHERE grid_config_id = ?
        """, [config_id]).fetchdf().to_dict("records")
        return rows[0] if rows else {
            "total_trades": 0, "winning_trades": 0, "total_net_pnl": 0,
            "avg_roi_pct": 0, "total_hold_seconds": 0,
        }

    def get_recent_trades(self, config_id: int, limit: int = 20) -> list[dict]:
        return self.conn.execute("""
            SELECT * FROM closed_trades
            WHERE grid_config_id = ?
            ORDER BY close_time DESC
            LIMIT ?
        """, [config_id, limit]).fetchdf().to_dict("records")

    def get_equity_history(self, config_id: int) -> list[dict]:
        """只有多策略时才用 config_id 过滤，简单实现直接取全部"""
        return self.conn.execute("""
            SELECT * FROM equity_curve
            ORDER BY timestamp
        """).fetchdf().to_dict("records")

    def export_to_csv(self, table: str, output_path: str):
        """导出表到 CSV"""
        self.conn.execute(f"COPY {table} TO '{output_path}' (HEADER, DELIMITER ',')")
        logger.info(f"📤 {table} 已导出到 {output_path}")

    def export_to_parquet(self, table: str, output_path: str):
        """导出表到 Parquet（压缩率更高）"""
        self.conn.execute(f"COPY {table} TO '{output_path}' (FORMAT PARQUET)")
        logger.info(f"📤 {table} 已导出到 {output_path}")


if __name__ == "__main__":
    # 测试数据库初始化
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db = Database("test_grid.db")
    db.connect()
    print("✅ Schema 创建成功")
    print("表列表:", db.conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchdf())
    db.close()
