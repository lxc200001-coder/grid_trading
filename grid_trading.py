"""
成交驱动型 IBKR 网格策略引擎
=============================
代码风格参考: 量化平台标准模式

核心逻辑:
  1. trigger_symbols()  — 定义驱动标的
  2. global_variables() — 定义参数 / 校验 / 初始化状态
  3. deploy()           — 以基准价为中心挂 1 对单 (BUY + SELL)
  4. 成交回调 + 补单:
       BUY 成交 → 撤 SELL → 成交价为新基准 → 重新挂 1 对单
       SELL 成交 → 撤 BUY → 成交价为新基准 → 重新挂 1 对单
  5. 每轮只有 2 个活跃订单, 基准价随成交动态移动

数据持久化: DuckDB (orders / fills / closed_trades / market_snapshots / equity_curve)
"""

import json
import logging
import math
import signal
import time
from pathlib import Path

from ib_insync import IB, Stock, LimitOrder, Trade

from db import Database

logger = logging.getLogger(__name__)


class OrderStatus:
    """订单状态枚举"""
    SUBMITTED   = "SUBMITTED"
    FILLED_ALL  = "FILLED"
    CANCELLED   = "CANCELLED"
    PENDING     = "PENDING"
    FAILED      = "FAILED"


class FillDrivenGrid:
    """
    成交驱动型网格策略引擎

    状态:
      每轮挂 1 对订单: BUY @ base - step, SELL @ base + step
      成交后撤反向单, 以成交价为新基准, 重新挂 1 对
    """

    # ---------------------------------------------------------------
    #  驱动标的
    # ---------------------------------------------------------------
    def trigger_symbols(self):
        """定义驱动标的"""
        self.驱动标的1 = self.cfg["symbol"]

    # ---------------------------------------------------------------
    #  全局变量 & 初始化
    # ---------------------------------------------------------------
    def global_variables(self):
        """定义全局变量、参数校验、初始化状态"""
        cfg = self.cfg

        # ---------- 可调参数 ----------
        self.初始基准价       = cfg.get("base_price", 0)
        self.股价每跌多少买入 = cfg.get("buy_step_price", 2.5)
        self.股价每涨多少卖出 = cfg.get("sell_step_price", 2.5)
        self.每笔委托股数     = cfg.get("qty_per_level", 100)
        self.策略最大持仓股数 = cfg.get("max_position", 500)

        # ---------- 运行时状态 ----------
        self.open_order_dict  = {}   # price -> db_order_id (BUY)
        self.close_order_dict = {}   # price -> db_order_id (SELL)
        self.策略当前持仓股数  = 0
        self.成交的开仓买单数量 = 0
        self.成交的平仓卖单数量 = 0

        # 当前基准价 (动态移动)
        self.current_base_price = self.初始基准价

        # 用于配对盈亏
        self._pending_buy_price = None   # 等待平仓的买入成交价
        self._pending_buy_order_id = None

        # ---------- 初始持仓 ----------
        self.初始持仓 = self._get_initial_position()
        self.每手股数  = self._get_lot_size()

        # ---------- 精度控制 ----------
        self.float_print_digits = cfg.get("precision", 10)

        def custom_round(number, ndigits=self.float_print_digits):
            multiplier = 10 ** ndigits
            if number * multiplier - math.floor(number * multiplier) >= 0.5:
                return math.ceil(number * multiplier) / multiplier
            return math.floor(number * multiplier) / multiplier
        self.round = custom_round

        # ---------- 撤单辅助 ----------
        _cannot_cancel_status = {
            OrderStatus.FILLED_ALL,
            OrderStatus.CANCELLED,
            OrderStatus.FAILED,
        }

        def cancel_order_in_dict(order_id):
            if not self.cfg["dry_run"] and order_id:
                status = self._ib_order_status(order_id)
                if status not in _cannot_cancel_status:
                    try:
                        self.ib.cancelOrder(order_id)
                        logger.info(f"  [CANCEL] 已撤单 orderId={order_id}")
                    except Exception as e:
                        logger.warning(f"撤单失败 orderId={order_id}: {e}")

        def cancel_all_and_quit():
            """撤销所有挂单并退出"""
            for price_key in list(self.open_order_dict.keys()):
                cancel_order_in_dict(self.open_order_dict[price_key])
            for price_key in list(self.close_order_dict.keys()):
                cancel_order_in_dict(self.close_order_dict[price_key])
            logger.warning("[BOUND] 策略已停止, 所有挂单已撤销")
            self.running = False
            self.stop()

        self.cancel_all_and_quit = cancel_all_and_quit

        # ---------- 启动前检查 ----------
        self._preflight_checks()

        # ---------- 启动日志 ----------
        logger.info(f"[CHART] 动态网格: BUY -{self.股价每跌多少买入} / "
                     f"SELL +{self.股价每涨多少卖出}  @ {self.每笔委托股数}股/笔")

    # ---------------------------------------------------------------
    #  校验
    # ---------------------------------------------------------------
    def _preflight_checks(self):
        """启动前参数校验"""
        # 1. 已有挂单检查
        if not self.cfg["dry_run"]:
            existing = self._get_open_orders()
            if existing:
                logger.warning(f"标的已有未成交挂单: {existing}")
                logger.warning("运行网格前请撤销标的挂单")
                self.cancel_all_and_quit()

        # 2. 参数校验
        errors = []
        if self.初始基准价 <= 0:
            errors.append("初始基准价需 > 0")
        if self.每笔委托股数 <= 0:
            errors.append("每笔委托股数需 > 0")
        if self.每笔委托股数 % self.每手股数 != 0:
            errors.append(f"每笔委托股数需为整手数 (每手 {self.每手股数} 股)")
        if self.策略最大持仓股数 < self.每笔委托股数:
            errors.append(f"策略最大持仓股数需 >= 每笔委托股数 {self.每笔委托股数}")
        if self.初始持仓 < 0:
            errors.append(f"{self.驱动标的1} 初始持仓不能为空仓")

        # 首层价格检查: 初始 BUY 价 <= 当前价
        buy_price = self.初始基准价 - self.股价每跌多少买入
        current_price = self._get_current_price()
        if buy_price > current_price:
            errors.append(
                f"首层BUY价 {buy_price:.2f} 需 <= 当前价 {current_price:.2f}"
                f"，请调整初始基准价或开仓步长"
            )

        if errors:
            for e in errors:
                logger.error(f"[ERR] {e}")
            self.cancel_all_and_quit()
            return

    # ---------------------------------------------------------------
    #  下单 / 撤单 / 重新挂对
    # ---------------------------------------------------------------
    def deploy(self):
        """部署初始 1 对订单: BUY @ base-step, SELL @ base+step"""
        self._place_pair(self.初始基准价)

    def _place_pair(self, base_price: float):
        """
        以 base_price 为基准, 挂 1 对订单:
          BUY  @ base_price - buy_step
          SELL @ base_price + sell_step
        """
        buy_price = self.round(base_price - self.股价每跌多少买入)
        sell_price = self.round(base_price + self.股价每涨多少卖出)

        # 先清除旧的对价记录
        self.open_order_dict.clear()
        self.close_order_dict.clear()

        self._place_order(buy_price, "BUY", is_open=True)
        self._place_order(sell_price, "SELL", is_open=False)

        self.current_base_price = base_price
        logger.info(f"[DPLY] 新基准 {base_price:.2f} | "
                     f"BUY {buy_price:.2f}  SELL {sell_price:.2f}")

    def _cancel_opposite_and_replace(self, filled_price: float, filled_action: str):
        """
        成交后处理:
          1. 撤反向单
          2. 以成交价为新基准, 重新挂 1 对
        """
        if filled_action == "BUY":
            # 撤 SELL 单
            for p, oid in list(self.close_order_dict.items()):
                logger.info(f"  [REB] BUY成交 → 撤SELL @ {p:.2f}")
                if not self.cfg["dry_run"]:
                    self._cancel_order(oid)
            self.close_order_dict.clear()
        else:
            # 撤 BUY 单
            for p, oid in list(self.open_order_dict.items()):
                logger.info(f"  [REB] SELL成交 → 撤BUY @ {p:.2f}")
                if not self.cfg["dry_run"]:
                    self._cancel_order(oid)
            self.open_order_dict.clear()

        # 以成交价为新基准, 重新挂对
        self._place_pair(filled_price)

    def _cancel_order(self, order_id):
        """撤销单个订单"""
        try:
            self.ib.cancelOrder(order_id)
        except Exception as e:
            logger.warning(f"撤单失败 orderId={order_id}: {e}")

    # ---------------------------------------------------------------
    #  下单
    # ---------------------------------------------------------------
    def _place_order(self, price: float, action: str, is_open: bool) -> int:
        """挂一个限价单, 记录到 DB + 字典"""
        qty = self.每笔委托股数

        order_rec = {
            "grid_config_id": self.config_id,
            "grid_level": 0,
            "action": action,
            "price": price,
            "quantity": qty,
            "status": "SUBMITTED",
        }

        if self.cfg["dry_run"]:
            logger.info(f"  [DRY] [{action}] {qty}股 @ {price:.2f} {'[开]' if is_open else '[平]'}")
            order_rec["ib_order_id"] = 0
            db_id = self.db.save_order(order_rec)
            target = self.open_order_dict if is_open else self.close_order_dict
            target[price] = db_id
            return db_id

        order = LimitOrder(action, qty, price)
        order.tif = "GTC"
        trade = self.ib.placeOrder(self.contract, order)
        order_rec["ib_order_id"] = trade.order.orderId
        db_id = self.db.save_order(order_rec)

        target = self.open_order_dict if is_open else self.close_order_dict
        target[price] = trade.order.orderId

        trade.fillEvent += lambda t: self._on_fill(t, price, action, is_open, db_id)
        logger.info(f"  [SEND] [{action}] {qty}股 @ {price:.2f} "
                     f"(orderId={trade.order.orderId})")
        return db_id

    # ---------------------------------------------------------------
    #  成交回调
    # ---------------------------------------------------------------
    def _on_fill(self, trade: Trade, price: float, action: str,
                  was_open: bool, db_order_id: int):
        """成交回调: 记录 + 配对盈亏 + 触发补单"""
        fill = trade.fills[-1]
        exec_ = fill.execution
        qty = int(exec_.shares)

        logger.info(f"[FILL] {exec_.side} {qty}股 @ {exec_.price:.2f}")

        # 1. 记录 fills + 更新 orders
        self.db.save_fill({
            "order_id": db_order_id,
            "ib_exec_id": exec_.execId,
            "action": exec_.side,
            "fill_price": exec_.price,
            "fill_qty": qty,
            "fill_time": exec_.time,
            "commission": exec_.commission if exec_.commission else 0,
        })
        self.db.update_order(db_order_id, {
            "status": "FILLED",
            "filled_qty": qty,
            "avg_fill_price": exec_.price,
            "filled_at": exec_.time,
        })

        # 2. 更新运行时状态
        target = self.open_order_dict if was_open else self.close_order_dict
        if price in target:
            del target[price]

        fill_price = float(exec_.price)
        if was_open:
            self.成交的开仓买单数量 += 1
            self.策略当前持仓股数 += qty
        else:
            self.成交的平仓卖单数量 += 1
            self.策略当前持仓股数 -= qty

        # 3. 配对盈亏: BUY成交记录, SELL成交配对
        if was_open:
            # BUY 成交 → 记录等 SELL 配对
            self._pending_buy_price = fill_price
            self._pending_buy_order_id = db_order_id
        else:
            # SELL 成交 → 与之前的 BUY 配对
            if self._pending_buy_price is not None:
                gross_pnl = (fill_price - self._pending_buy_price) * qty
                hold_sec = (exec_.time - self.db.conn.execute(
                    "SELECT filled_at FROM orders WHERE id = ?",
                    [self._pending_buy_order_id],
                ).fetchone()[0]).total_seconds()
                cost = self._pending_buy_price * qty
                roi = (gross_pnl / cost * 100) if cost > 0 else 0
                self.db.close_trade({
                    "grid_config_id": self.config_id,
                    "buy_order_id": self._pending_buy_order_id,
                    "sell_order_id": db_order_id,
                    "symbol": self.cfg["symbol"],
                    "buy_price": self._pending_buy_price,
                    "sell_price": fill_price,
                    "quantity": qty,
                    "gross_pnl": round(gross_pnl, 2),
                    "net_pnl": round(gross_pnl, 2),
                    "roi_pct": round(roi, 4),
                    "open_time": self.db.conn.execute(
                        "SELECT filled_at FROM orders WHERE id = ?",
                        [self._pending_buy_order_id],
                    ).fetchone()[0],
                    "close_time": exec_.time,
                    "hold_seconds": int(hold_sec),
                })
                logger.info(f"[PNL] 配对完成: BUY {self._pending_buy_price:.2f} → "
                             f"SELL {fill_price:.2f} | 毛利 {gross_pnl:.2f}")
                self._pending_buy_price = None
                self._pending_buy_order_id = None

        # 4. 核心: 撤反向单 + 以成交价为基准重新挂对
        self._cancel_opposite_and_replace(fill_price, action)

    # ---------------------------------------------------------------
    #  IBKR 辅助
    # ---------------------------------------------------------------
    def _get_current_price(self):
        """current_price()"""
        if self.cfg["dry_run"] or not self.ib:
            return self.cfg.get("base_price", 440.0)
        ticker = self.ib.reqMktData(self.contract, "", False, False)
        self.ib.sleep(0.5)
        p = ticker.marketPrice()
        return round(float(p or (ticker.bid + ticker.ask) / 2), 2)

    def _get_initial_position(self):
        """position_holding_qty()"""
        if self.cfg["dry_run"] or not self.ib:
            return 0
        for pos in self.ib.positions():
            if pos.contract.symbol == self.cfg["symbol"]:
                return int(pos.position)
        return 0

    def _get_lot_size(self):
        """lot_size()"""
        if not self.contract or self.cfg["dry_run"]:
            return 1
        details = self.ib.reqContractDetails(self.contract)
        return details[0].contract.multiplier if details else 1

    def _get_open_orders(self):
        """获取标的已有挂单"""
        if not self.ib or self.cfg["dry_run"]:
            return []
        trades = self.ib.reqAllOpenOrders()
        return [t for t in trades if t.contract.symbol == self.cfg["symbol"]]

    def _ib_order_status(self, order_id):
        """查询 IBKR 订单状态"""
        if not self.ib or self.cfg["dry_run"]:
            return OrderStatus.PENDING
        try:
            trades = self.ib.reqAllOpenOrders()
            for t in trades:
                if t.order.orderId == order_id:
                    return OrderStatus.SUBMITTED
            return OrderStatus.FILLED_ALL
        except Exception:
            return OrderStatus.PENDING

    # ---------------------------------------------------------------
    #  定时任务
    # ---------------------------------------------------------------
    def _record_snapshot(self):
        if self.cfg["dry_run"] or not self.ib:
            return
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
        except Exception:
            pass

    def _record_equity(self):
        if self.cfg["dry_run"] or not self.ib:
            return
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
        except Exception:
            pass

    # ---------------------------------------------------------------
    #  主循环
    # ---------------------------------------------------------------
    def run_forever(self):
        """主事件循环"""
        self.running = True
        self.deploy()
        logger.info("[WAIT] 动态网格运行中 (Ctrl+C 停止)...")

        snap_int = self.cfg.get("snapshot_interval_s", 5)
        eq_int   = self.cfg.get("equity_interval_s", 60)
        _last_snap = 0.0
        _last_eq   = 0.0

        while self.running:
            try:
                if self.ib and not self.cfg["dry_run"]:
                    self.ib.sleep(1)
                else:
                    time.sleep(1)
                t = time.time()
                if t - _last_snap >= snap_int:
                    self._record_snapshot()
                    _last_snap = t
                if t - _last_eq >= eq_int:
                    self._record_equity()
                    _last_eq = t
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.exception(f"[EXCEPT] {e}")
                time.sleep(5)
        self.stop()

    # ---------------------------------------------------------------
    #  生命周期
    # ---------------------------------------------------------------
    def initialize(self) -> "FillDrivenGrid":
        """连接 IBKR + DuckDB, 初始化策略"""
        cfg = self.cfg
        self.db = Database(cfg["db_path"]).connect()
        db_cfg = {
            "name": cfg.get("name", "grid"),
            "symbol": cfg["symbol"],
            "exchange": cfg.get("exchange", "SMART"),
            "currency": cfg.get("currency", "USD"),
            "sec_type": cfg.get("sec_type", "STK"),
            "lower_price": round(cfg.get("base_price", 440) - 10, 2),
            "upper_price": round(cfg.get("base_price", 440) + 10, 2),
            "grid_levels": 1,
            "qty_per_level": cfg["qty_per_level"],
            "max_position": cfg.get("max_position", 0),
            "status": "ACTIVE",
            "dry_run": cfg.get("dry_run", True),
        }
        self.config_id = self.db.save_config(db_cfg)

        if not cfg["dry_run"]:
            self.ib = IB()
            self.ib.connect(
                cfg["ibkr_host"], cfg["ibkr_port"],
                clientId=cfg["ibkr_client_id"],
            )
            self.ib.errorEvent += self._on_error
            self.contract = Stock(
                cfg["symbol"], cfg["exchange"], cfg["currency"],
            )
            self.ib.qualifyContracts(self.contract)
            logger.info("[OK] IBKR 已连接")
        else:
            self.ib = None
            self.contract = None
            logger.info("[DRY] DRY RUN 模式")

        self.trigger_symbols()
        self.global_variables()
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        return self

    def stop(self):
        """停止策略"""
        logger.info("[STOP] 停止网格策略...")
        self.running = False
        self._print_summary()
        if self.db:
            self.db.update_config_status(self.config_id, "STOPPED")
        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
            logger.info("[DISC] IBKR 已断开")
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
        logger.info("[CHART] 动态网格 运行汇总")
        logger.info(f"   总订单: {n_orders}  |  成交笔数: {n_fills}")
        logger.info(f"   完成循环: {summary['total_trades']}")
        logger.info(f"   盈利次数: {summary['winning_trades']}")
        logger.info(f"   总盈亏:   {summary['total_net_pnl']:.2f}")
        logger.info(f"   平均ROI:  {summary['avg_roi_pct']:.4f}%")
        logger.info(f"   当前持仓: {self.策略当前持仓股数} 股")
        logger.info("=" * 52)

    def _on_error(self, _req_id, error_code, error_string, *_):
        logger.error(f"[ERR] IBKR Error (code={error_code}): {error_string}")

    def _handle_signal(self, signum, _frame):
        logger.info(f"[SIG] 信号 {signum}, 退出...")
        self.running = False


# ================================================================
#  入口
# ================================================================

DEFAULT_CONFIG = {
    "ibkr_host": "127.0.0.1",
    "ibkr_port": 7497,
    "ibkr_client_id": 1,
    "symbol": "SPY",
    "sec_type": "STK",
    "exchange": "SMART",
    "currency": "USD",
    "name": "SPY_dynamic_grid",
    "base_price": 440.0,
    "buy_step_price": 2.5,
    "sell_step_price": 2.5,
    "qty_per_level": 100,
    "max_position": 500,
    "precision": 10,
    "dry_run": True,
    "snapshot_interval_s": 5,
    "equity_interval_s": 60,
    "db_path": "grid_trading.duckdb",
}


def _setup_logging():
    """配置日志"""
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    file_handler = logging.FileHandler("grid_trading.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(log_format))
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[
        logging.StreamHandler(), file_handler,
    ])


def run(config_override: dict | None = None, config_file: str | None = None):
    cfg = dict(DEFAULT_CONFIG)
    if config_file and Path(config_file).exists():
        with open(config_file) as f:
            cfg.update(json.load(f))
    if config_override:
        cfg.update(config_override)

    engine = FillDrivenGrid()
    engine.cfg = cfg
    try:
        engine.initialize()
        engine.run_forever()
    except Exception as e:
        logger.exception(f"[EXCEPT] 策略异常退出: {e}")
        engine.stop()


if __name__ == "__main__":
    _setup_logging()

    # ====== 在这里改配置 ======
    MY_CONFIG = {
        "name": "SPY_动态网格",
        "symbol": "SPY",
        "base_price": 440.0,
        "buy_step_price": 2.5,
        "sell_step_price": 2.5,
        "qty_per_level": 100,
        "max_position": 500,
        "dry_run": True,
    }
    # ===========================

    run(MY_CONFIG)
