"""
IBKR 成交驱动型网格策略引擎
============================
核心逻辑:
  1. 初始部署: 当前价下方全挂 BUY, 上方全挂 SELL
  2. BUY 成交 → 在上一层 (i+1) 挂 SELL 锁利
  3. SELL 成交 → 在下一层 (i-1) 挂 BUY 接回
  4. 每层同时只存在一个订单, 成交后自动反向补单

数据全部写入 DuckDB, 支持 dry-run 模拟。
"""

import json
import logging
import time
import signal
from pathlib import Path
from typing import Optional

from ib_insync import IB, Stock, LimitOrder, Trade, Contract

from db import Database

logger = logging.getLogger(__name__)

# ================================================================
#  默认配置
# ================================================================
DEFAULT_CONFIG = {
    # --- IBKR ---
    "ibkr_host": "127.0.0.1",
    "ibkr_port": 7497,
    "ibkr_client_id": 1,
    # --- 标的 ---
    "symbol": "SPY",
    "sec_type": "STK",
    "exchange": "SMART",
    "currency": "USD",
    # --- 网格 ---
    "name": "SPY_fill_grid",
    "lower_price": 400.0,
    "upper_price": 460.0,
    "grid_levels": 12,
    "qty_per_level": 10,
    # --- 风控 ---
    "max_position": 200,
    "stop_loss_pct": 0.05,
    # --- 运行 ---
    "dry_run": True,
    "snapshot_interval_s": 5,
    "equity_interval_s": 60,
    # --- 数据库 ---
    "db_path": "grid_trading.duckdb",
}


class FillDrivenGrid:
    """
    成交驱动型网格引擎

    状态机 (每层):
      EMPTY → 挂 BUY / 挂 SELL → FILLED → 挂反向单 → 循环
    """

    def __init__(self, config: dict | None = None):
        self.cfg = {**DEFAULT_CONFIG, **(config or {})}
        self.db: Database | None = None
        self.ib: IB | None = None
        self.contract: Contract | None = None
        self.config_id: int | None = None

        # --- 网格数据 ---
        self.grid_prices: list[float] = []

        # 每层当前状态: { price: { "level": int, "action": "BUY"|"SELL",
        #                          "db_order_id": int, "ib_trade": Trade|None,
        #                          "filled_qty": int } }
        self.levels: dict[float, dict] = {}

        # 运行时
        self.running = False
        self._last_equity_ts = 0.0
        self._last_snapshot_ts = 0.0

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # ================================================================
    #  初始化
    # ================================================================

    def initialize(self) -> "FillDrivenGrid":
        self.db = Database(self.cfg["db_path"]).connect()
        self.config_id = self.db.save_config(self.cfg)

        if not self.cfg["dry_run"]:
            self.ib = IB()
            self.ib.connect(
                self.cfg["ibkr_host"],
                self.cfg["ibkr_port"],
                clientId=self.cfg["ibkr_client_id"],
            )
            self.ib.errorEvent += self._on_error
            self.contract = Stock(
                self.cfg["symbol"], self.cfg["exchange"], self.cfg["currency"],
            )
            self.ib.qualifyContracts(self.contract)
            logger.info(f"✅ IBKR 已连接 — 账户: {self.ib.managedAccounts()}")
        else:
            logger.info("🧪 DRY RUN 模式 — 不下真实订单")

        self.grid_prices = self._build_grid()
        logger.info(f"📊 网格 {self.cfg['grid_levels']} 层: "
                     f"{self.grid_prices[0]:.2f} ~ {self.grid_prices[-1]:.2f}")
        return self

    def _build_grid(self) -> list[float]:
        lo, hi = self.cfg["lower_price"], self.cfg["upper_price"]
        n = self.cfg["grid_levels"]
        step = (hi - lo) / (n - 1)
        return [round(lo + i * step, 2) for i in range(n)]

    def _get_current_price(self) -> float:
        """获取当前市价 (dry-run 返回网格中间价)"""
        if self.cfg["dry_run"] or not self.ib:
            mid = (self.cfg["lower_price"] + self.cfg["upper_price"]) / 2
            return round(mid, 2)
        ticker = self.ib.reqMktData(self.contract, "", False, False)
        self.ib.sleep(0.5)
        p = ticker.marketPrice()
        if not p or p <= 0:
            p = (ticker.bid + ticker.ask) / 2
        return round(float(p), 2)

    # ================================================================
    #  核心: 部署网格
    # ================================================================

    def deploy(self):
        """
        部署初始网格:
          - 低于市价 → 挂 BUY
          - 高于市价 → 挂 SELL
        """
        current_price = self._get_current_price()
        logger.info(f"🎯 当前价格: {current_price:.2f}")

        for i, price in enumerate(self.grid_prices):
            if price < current_price:
                action = "BUY"
            elif price > current_price:
                action = "SELL"
            else:
                logger.info(f"  ⏭ 跳过当前价层 {price:.2f}")
                continue

            self._place_order(i, price, action)

        active = sum(1 for v in self.levels.values() if v.get("db_order_id"))
        logger.info(f"🚀 网格已部署: {active} 层待成交")
        self.running = True

    # ================================================================
    #  下单 / 补单
    # ================================================================

    def _place_order(self, level: int, price: float, action: str) -> Optional[int]:
        """
        挂一个限价单, 记录到 DB 和 self.levels

        Returns: db_order_id
        """
        qty = self.cfg["qty_per_level"]

        # --- 数据库记录 ---
        order_rec = {
            "grid_config_id": self.config_id,
            "grid_level": level,
            "action": action,
            "price": price,
            "quantity": qty,
            "status": "SUBMITTED",
        }

        if self.cfg["dry_run"]:
            logger.info(f"  🧪 [{action}] {qty}股 @ {price:.2f} (层{level})")
            order_rec["ib_order_id"] = 0
            db_id = self.db.save_order(order_rec)
            self.levels[price] = {
                "level": level,
                "action": action,
                "db_order_id": db_id,
                "ib_trade": None,
                "filled_qty": 0,
                "price": price,
            }
            return db_id

        # --- 实盘 ---
        order = LimitOrder(action, qty, price)
        order.tif = "GTC"
        order.outsideRth = False

        trade = self.ib.placeOrder(self.contract, order)
        order_rec["ib_order_id"] = trade.order.orderId
        db_id = self.db.save_order(order_rec)

        self.levels[price] = {
            "level": level,
            "action": action,
            "db_order_id": db_id,
            "ib_trade": trade,
            "filled_qty": 0,
            "price": price,
        }

        trade.fillEvent += lambda t: self._on_fill(t, price, level)
        logger.info(f"  📤 [{action}] {qty}股 @ {price:.2f} (层{level}, "
                     f"orderId={trade.order.orderId})")
        return db_id

    def _rebalance(self, _filled_price: float, filled_action: str, level: int):
        """
        成交后补反向单:

        规则:
          BUY 成交 → 在 *上一层* (level+1) 挂 SELL, 获利了结
          SELL 成交 → 在 *下一层* (level-1) 挂 BUY, 低价接回

        边界: 如果超出网格范围则忽略 (网格可扩展, 但这里先保守)
        """
        if filled_action == "BUY":
            target_level = level + 1
            new_action = "SELL"
        else:  # SELL
            target_level = level - 1
            new_action = "BUY"

        # 边界检查
        if target_level < 0 or target_level >= len(self.grid_prices):
            logger.info(f"  ⛔ 层{target_level} 超出网格边界, 不补单")
            return

        target_price = self.grid_prices[target_level]

        # 检查该层是否已有活跃订单
        existing = self.levels.get(target_price)
        if existing and existing.get("db_order_id"):
            # 检查该订单是否仍有效 (可能已取消或已成交)
            logger.info(f"  ⏭ 层{target_level} ({target_price:.2f}) 已有订单, 跳过")
            return

        logger.info(f"  🔄 [{new_action}] @ {target_price:.2f} (层{target_level}) "
                     f"← 因层{level} {filled_action} 成交")
        self._place_order(target_level, target_price, new_action)

    # ================================================================
    #  成交回调
    # ================================================================

    def _on_fill(self, trade: Trade, price: float, level: int):
        """IBKR 成交回调"""
        fill = trade.fills[-1]
        exec_ = fill.execution
        logger.info(f"💹 成交: {exec_.side} {exec_.shares}股 @ {exec_.price:.2f}")

        # 1. 更新 DB (fills + orders)
        db_order_id = self.levels.get(price, {}).get("db_order_id")
        if db_order_id:
            self.db.save_fill({
                "order_id": db_order_id,
                "ib_exec_id": exec_.execId,
                "action": exec_.side,
                "fill_price": exec_.price,
                "fill_qty": int(exec_.shares),
                "fill_time": exec_.time,
                "commission": exec_.commission if exec_.commission else 0,
            })
            self.db.update_order(db_order_id, {
                "status": "FILLED",
                "filled_qty": int(exec_.shares),
                "avg_fill_price": exec_.price,
                "filled_at": exec_.time,
            })

        # 2. 标记该层为已成交
        if price in self.levels:
            self.levels[price]["filled_qty"] = (
                self.levels[price].get("filled_qty", 0) + int(exec_.shares)
            )
            self.levels[price]["db_order_id"] = None  # 清除引用, 允许重挂

        # 3. 成交驱动: 补反向单
        self._rebalance(price, exec_.side, level)

    def _on_error(self, req_id, error_code, error_string, *_):
        logger.error(f"❌ IBKR Error (code={error_code}): {error_string}")

    # ================================================================
    #  定时任务
    # ================================================================

    def _record_snapshot(self):
        if self.cfg["dry_run"] or not self.ib:
            return
        t = time.time()
        if t - self._last_snapshot_ts < self.cfg["snapshot_interval_s"]:
            return
        self._last_snapshot_ts = t
        try:
            ticker = self.ib.reqMktData(self.contract, "", False, False)
            self.ib.sleep(0.3)
            self.db.save_market_snapshot(
                self.cfg["symbol"],
                ticker.marketPrice(),
                ticker.bid,
                ticker.ask,
                int(ticker.volume or 0),
            )
        except Exception as e:
            logger.warning(f"⚠️ 行情快照失败: {e}")

    def _record_equity(self):
        if self.cfg["dry_run"] or not self.ib:
            return
        t = time.time()
        if t - self._last_equity_ts < self.cfg["equity_interval_s"]:
            return
        self._last_equity_ts = t
        try:
            acct = self.ib.accountSummary()
            summary = {item.tag: item.value for item in acct}
            pos = self.ib.positions()
            pos_value = sum(p.marketValue for p in pos) if pos else 0
            self.db.save_equity_snapshot({
                "total_value": float(summary.get("NetLiquidation", 0)),
                "cash": float(summary.get("TotalCashValue", 0)),
                "position_value": float(pos_value),
                "unrealized_pnl": float(summary.get("UnrealizedPnL", 0)),
                "realized_pnl": float(summary.get("RealizedPnL", 0)),
                "margin_used": float(summary.get("GrossPositionValue", 0)),
            })
        except Exception as e:
            logger.warning(f"⚠️ 权益快照失败: {e}")

    # ================================================================
    #  主循环
    # ================================================================

    def run_forever(self):
        self.deploy()
        logger.info("⏳ 成交驱动网格运行中 (Ctrl+C 停止)...")
        while self.running:
            try:
                if self.ib and not self.cfg["dry_run"]:
                    self.ib.sleep(1)
                else:
                    time.sleep(1)
                self._record_snapshot()
                self._record_equity()
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.exception(f"💥 异常: {e}")
                time.sleep(5)
        self.stop()

    # ================================================================
    #  停止 & 汇总
    # ================================================================

    def stop(self):
        logger.info("⏹ 停止网格策略...")
        self.running = False

        if self.cfg["dry_run"]:
            logger.info("  [DRY] 模拟结束")

        self._print_summary()

        if self.db:
            self.db.update_config_status(self.config_id, "STOPPED")

        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
            logger.info("🔌 IBKR 已断开")

        if self.db:
            self.db.close()

    def _print_summary(self):
        if not self.db:
            return
        summary = self.db.get_pnl_summary(self.config_id)
        n_orders = self.db.conn.execute(
            "SELECT COUNT(*) FROM orders WHERE grid_config_id = ?",
            [self.config_id],
        ).fetchone()[0]
        n_fills = self.db.conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id IN "
            "(SELECT id FROM orders WHERE grid_config_id = ?)",
            [self.config_id],
        ).fetchone()[0]
        logger.info("=" * 52)
        logger.info("📊 成交驱动网格 运行汇总")
        logger.info(f"   总订单: {n_orders}  |  成交笔数: {n_fills}")
        logger.info(f"   已平仓配对: {summary['total_trades']}")
        logger.info(f"   盈利笔数:   {summary['winning_trades']}")
        logger.info(f"   总盈亏:     {summary['total_net_pnl']:.2f}")
        logger.info(f"   平均ROI:    {summary['avg_roi_pct']:.4f}%")
        logger.info("=" * 52)

    def _handle_signal(self, signum, _frame):
        logger.info(f"📥 信号 {signum}, 退出...")
        self.running = False


# ================================================================
#  入口
# ================================================================

def run(config_override: dict | None = None, config_file: str | None = None):
    cfg = dict(DEFAULT_CONFIG)
    if config_file:
        path = Path(config_file)
        if path.exists():
            with open(path) as f:
                cfg.update(json.load(f))
    if config_override:
        cfg.update(config_override)

    engine = FillDrivenGrid(cfg)
    try:
        engine.initialize()
        engine.run_forever()
    except Exception as e:
        logger.exception(f"💥 策略异常退出: {e}")
        engine.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("grid_trading.log")],
    )

    # ====== 在这里改配置 ======
    MY_CONFIG = {
        "name": "SPY_成交驱动网格",
        "symbol": "SPY",
        "lower_price": 400.0,
        "upper_price": 460.0,
        "grid_levels": 12,
        "qty_per_level": 10,
        "dry_run": True,              # 先 dry-run 验证
    }
    # ===========================

    run(MY_CONFIG)
