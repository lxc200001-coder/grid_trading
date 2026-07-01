"""
成交驱动型 IBKR 网格策略引擎
=============================
代码风格参考: 量化平台标准模式

核心逻辑:
  1. trigger_symbols()  — 定义驱动标的
  2. global_variables() — 定义参数 / 校验 / 计算网格 / 初始化状态
  3. run_forever()      — 事件循环: 成交驱动补单 + 定时快照

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

# ================================================================
#  常量 & 工具
# ================================================================

class OrderStatus:
    """订单状态枚举 (兼容 ib_insync + 统一常量)"""
    SUBMITTED   = "SUBMITTED"
    FILLED_ALL  = "FILLED"
    CANCELLED   = "CANCELLED"
    PENDING     = "PENDING"
    FAILED      = "FAILED"


# ================================================================
#  网格策略引擎
# ================================================================

class FillDrivenGrid:
    """成交驱动型网格策略引擎"""

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

        # ---------- 可调参数 (show_variable 风格) ----------
        self.初始基准价        = cfg.get("base_price", 0)
        self.按价格or按比例   = cfg.get("price_mode", 0)      # 0: 按价格  1: 按比例
        self.股价每跌多少买入 = cfg.get("buy_step_price", 1)  # 每层买入价差
        self.股价每涨多少卖出 = cfg.get("sell_step_price", 1) # 每层卖出价差
        self.每笔委托股数     = cfg.get("qty_per_level", 100)
        self.策略最大持仓股数 = cfg.get("max_position", 500)

        # ---------- 运行时状态 ----------
        self.open_order_dict  = {}   # price -> db_order_id  (开仓挂单)
        self.close_order_dict = {}   # price -> db_order_id  (平仓挂单)
        self.place_open_qty   = 0    # 实际开仓挂单数量
        self.place_close_qty  = 0    # 实际平仓挂单数量
        self.tar_open_qty     = 0    # 目标开仓挂单数量
        self.tar_close_qty    = 0    # 目标平仓挂单数量

        self.策略当前持仓股数           = 0
        self.完全成交的开仓买单数量     = 0
        self.完全成交的平仓卖单数量     = 0

        # ---------- 从 IBKR 获取 ----------
        self.初始持仓  = self._get_initial_position()
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
            """安全撤销字典中记录的订单"""
            if not self.cfg["dry_run"]:
                status = self._ib_order_status(order_id)
                if status not in _cannot_cancel_status:
                    try:
                        self.ib.cancelOrder(order_id)
                        logger.info(f"  🗑 已撤单 orderId={order_id}")
                    except Exception as e:
                        logger.warning(f"撤单失败 orderId={order_id}: {e}")

        def cancel_strategy_orders_and_quit():
            """撤销所有挂单并退出"""
            for price_key in list(self.open_order_dict.keys()):
                cancel_order_in_dict(self.open_order_dict[price_key])
            for price_key in list(self.close_order_dict.keys()):
                cancel_order_in_dict(self.close_order_dict[price_key])
            logger.warning("⛔ 策略已停止，所有挂单已撤销")
            self.running = False
            self.stop()

        self.cancel_strategy_orders_and_quit_function = cancel_strategy_orders_and_quit

        # ---------- 计算层数 ----------
        self._layer_count = self.策略最大持仓股数 // self.每笔委托股数
        self._max_holding_position = self._layer_count * self.每笔委托股数

        # ---------- 启动前检查 ----------
        self._preflight_checks()

        # ---------- 计算网格价格 ----------
        self._build_grid()

        # ---------- 启动日志 ----------
        logger.info(f"📊 网格 {self._layer_count} 层: "
                     f"{self.layer_prices[0]:.2f} ~ {self.layer_prices[-1]:.2f}")

    # ---------------------------------------------------------------
    #  校验
    # ---------------------------------------------------------------
    def _preflight_checks(self):
        """启动前参数校验"""
        cfg = self.cfg
        _layer_count = self._layer_count

        # 1. 已有挂单检查
        if not cfg["dry_run"]:
            existing = self._get_open_orders()
            if existing:
                logger.warning(f"标的已有未成交挂单: {existing}")
                logger.warning("运行网格前请撤销标的挂单")
                self.cancel_strategy_orders_and_quit_function()

        # 2. 参数校验
        checks = [
            (self.策略最大持仓股数 < self.每笔委托股数,
             f"策略最大持仓股数需 ≥ 每笔委托股数 {self.每笔委托股数}"),
            (self.初始基准价 <= 0, "初始基准价需 > 0"),
            (self.每笔委托股数 % self.每手股数 != 0,
             f"每笔委托股数需为整手数 (每手 {self.每手股数} 股)"),
            (self.每笔委托股数 <= 0, "每笔委托股数需 > 0"),
            (self.初始持仓 < 0, f"{self.驱动标的1} 初始持仓不能为空仓"),
            (_layer_count > 15, "开仓层数不得超过 15 层"),
            (self.按价格or按比例 not in (0, 1), "按价格or按比例: 0=按价格, 1=按比例"),
        ]
        if self.按价格or按比例 == 0:
            max_buy_layers = int(self.初始基准价 / self.股价每跌多少买入)
            checks.append(
                (_layer_count > max_buy_layers,
                 f"最大持仓层数({_layer_count}) > 初始基准价/开仓网格宽度({max_buy_layers})"
                 f"，请减少最大持仓股数或减小开仓网格宽度")
            )
        else:
            checks.append(
                (not (0 < self.股价每跌多少买入 < 1),
                 "买入比例需为 0~1 之间的浮点数(不包含0和1)")
            )
            checks.append(
                (not (0 < self.股价每涨多少卖出 < 1),
                 "卖出比例需为 0~1 之间的浮点数(不包含0和1)")
            )

        for cond, msg in checks:
            if cond:
                logger.error(f"❌ {msg}")
                self.cancel_strategy_orders_and_quit_function()
                return

        # 3. 首层价格 ≤ 当前价
        _first_layer_price = self.初始基准价 - self.股价每跌多少买入
        if self.按价格or按比例 == 1:
            _first_layer_price = self.初始基准价 * ((1 - self.股价每跌多少买入) ** 1)
        _current_price = self._get_current_price()
        if _first_layer_price > _current_price:
            logger.error(
                f"❌ 首层挂单价 {_first_layer_price:.2f} 需 ≤ 当前价 {_current_price:.2f}"
                f"，请调整初始基准价或开仓网格宽度"
            )
            self.cancel_strategy_orders_and_quit_function()

        # 4. 购买力检查
        max_need = self._max_holding_position
        buying_power = self._get_buying_power()
        if buying_power is not None and max_need > buying_power:
            logger.error(
                f"❌ 所需购买力 {max_need} 股 > 最大购买力 {buying_power} 股"
                f"，请减少最大持仓股数或提升购买力"
            )
            self.cancel_strategy_orders_and_quit_function()

    # ---------------------------------------------------------------
    #  网格计算
    # ---------------------------------------------------------------
    def _build_grid(self):
        """按价格 / 按比例计算各层价格"""
        prices = []
        for i in range(self._layer_count):
            if self.按价格or按比例 == 0:
                price = self.初始基准价 - (i + 1) * self.股价每跌多少买入
            else:
                price = self.初始基准价 * ((1 - self.股价每跌多少买入) ** (i + 1))
            prices.append(self.round(price))

        self.layer_prices = prices
        self.卖出层价格 = [
            self.round(self.初始基准价 + (i + 1) * self.股价每涨多少卖出)
            for i in range(self._layer_count)
        ] if self.按价格or按比例 == 0 else [
            self.round(self.初始基准价 * ((1 + self.股价每涨多少卖出) ** (i + 1)))
            for i in range(self._layer_count)
        ]

        # 打印网格
        price_str = ", ".join(str(p) for p in prices)
        logger.info(f"📋 买入层: {price_str}")
        logger.info(f"📋 策略将在各层挂单 {self.每笔委托股数} 股, "
                     f"最大持仓 {self._max_holding_position} 股")

    # ---------------------------------------------------------------
    #  IBKR 辅助 (封装平台 API 风格)
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
        positions = self.ib.positions()
        for pos in positions:
            if pos.contract.symbol == self.cfg["symbol"]:
                return int(pos.position)
        return 0

    def _get_lot_size(self):
        """lot_size()"""
        if not self.contract or self.cfg["dry_run"]:
            return 1
        details = self.ib.reqContractDetails(self.contract)
        return details[0].contract.multiplier if details else 1

    def _get_buying_power(self):
        """max_qty_to_buy_on_margin()"""
        if self.cfg["dry_run"] or not self.ib:
            return None
        acct = self.ib.accountSummary()
        summary = {item.tag: item.value for item in acct}
        total = float(summary.get("NetLiquidation", 0))
        return int(total * 0.5 / self.layer_prices[0]) if self.layer_prices else None

    def _get_open_orders(self):
        """request_orderid()"""
        if not self.ib or self.cfg["dry_run"]:
            return []
        trades = self.ib.reqAllOpenOrders()
        return [t for t in trades if t.contract.symbol == self.cfg["symbol"]]

    def _ib_order_status(self, order_id):
        """查询 IBKR 订单状态"""
        if not self.ib or self.cfg["dry_run"]:
            return OrderStatus.PENDING
        try:
            trade = self.ib.reqAllOpenOrders()
            for t in trade:
                if t.order.orderId == order_id:
                    return OrderStatus.SUBMITTED
            return OrderStatus.FILLED_ALL
        except Exception:
            return OrderStatus.PENDING

    # ---------------------------------------------------------------
    #  下单
    # ---------------------------------------------------------------
    def _place_order(self, price: float, action: str, layer_idx: int = None,
                     is_open: bool = True) -> int:
        """
        挂单并记录到字典 + DB

        Returns: db_order_id
        """
        qty = self.每笔委托股数

        # DB 记录
        order_rec = {
            "grid_config_id": self.config_id,
            "grid_level": layer_idx if layer_idx is not None else -1,
            "action": action,
            "price": price,
            "quantity": qty,
            "status": "SUBMITTED",
        }

        if self.cfg["dry_run"]:
            logger.info(f"  🧪 [{action}] {qty}股 @ {price:.2f} "
                         f"{'[开]' if is_open else '[平]'}")
            order_rec["ib_order_id"] = 0
            db_id = self.db.save_order(order_rec)
            target_dict = self.open_order_dict if is_open else self.close_order_dict
            target_dict[price] = db_id
            if is_open:
                self.place_open_qty += 1
            else:
                self.place_close_qty += 1
            return db_id

        # 实盘
        order = LimitOrder(action, qty, price)
        order.tif = "GTC"
        trade = self.ib.placeOrder(self.contract, order)
        order_rec["ib_order_id"] = trade.order.orderId
        db_id = self.db.save_order(order_rec)

        # 记录到字典
        target_dict = self.open_order_dict if is_open else self.close_order_dict
        target_dict[price] = trade.order.orderId
        if is_open:
            self.place_open_qty += 1
        else:
            self.place_close_qty += 1

        # 成交回调
        trade.fillEvent += lambda t: self._on_fill(
            t, price, action, is_open, db_id,
        )

        logger.info(f"  📤 [{action}] {qty}股 @ {price:.2f} "
                     f"(orderId={trade.order.orderId})")
        return db_id

    # ---------------------------------------------------------------
    #  成交回调
    # ---------------------------------------------------------------
    def _on_fill(self, trade: Trade, price: float, action: str,
                  was_open: bool, db_order_id: int):
        """成交回调: 记录 + 反向补单"""
        fill = trade.fills[-1]
        exec_ = fill.execution
        qty = int(exec_.shares)

        logger.info(f"💹 成交: {exec_.side} {qty}股 @ {exec_.price:.2f}")

        # 1. 更新 DB
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
        target_dict = self.open_order_dict if was_open else self.close_order_dict
        if price in target_dict:
            del target_dict[price]
        if was_open:
            self.place_open_qty -= 1
            self.完全成交的开仓买单数量 += 1
            self.策略当前持仓股数 += qty
        else:
            self.place_close_qty -= 1
            self.完全成交的平仓卖单数量 += 1
            self.策略当前持仓股数 -= qty

        # 3. 成交驱动补单
        self._rebalance(price, action, was_open)

    # ---------------------------------------------------------------
    #  补单 (核心成交驱动逻辑)
    # ---------------------------------------------------------------
    def _rebalance(self, filled_price: float, _filled_action: str, was_open: bool):
        """
        成交后补单:
          - 开仓买单成交  →  在最近卖出层挂平仓卖单 (锁利)
          - 平仓卖单成交  →  在最近买入层挂开仓买单 (接回)
        """
        if was_open:
            # 买单成交 → 找对应的卖出价
            if self.按价格or按比例 == 0:
                sell_price = self.round(filled_price + self.股价每涨多少卖出)
            else:
                sell_price = self.round(filled_price * (1 + self.股价每涨多少卖出))

            if sell_price not in self.close_order_dict:
                logger.info(f"  🔄 BUY成交 → 挂SELL @ {sell_price:.2f} 锁利")
                self._place_order(sell_price, "SELL", is_open=False)
            else:
                logger.info(f"  ⏭ SELL@{sell_price:.2f} 已有挂单, 跳过")
        else:
            # 卖单成交 → 找对应的买入价
            if self.按价格or按比例 == 0:
                buy_price = self.round(filled_price - self.股价每跌多少买入)
            else:
                buy_price = self.round(filled_price * (1 - self.股价每跌多少买入))

            if buy_price not in self.open_order_dict:
                logger.info(f"  🔄 SELL成交 → 挂BUY @ {buy_price:.2f} 接回")
                self._place_order(buy_price, "BUY", is_open=True)
            else:
                logger.info(f"  ⏭ BUY@{buy_price:.2f} 已有挂单, 跳过")

    # ---------------------------------------------------------------
    #  部署
    # ---------------------------------------------------------------
    def deploy(self):
        """部署初始网格: 在买入层挂 BUY"""
        self.tar_open_qty = len(self.layer_prices)

        for i, price in enumerate(self.layer_prices):
            self._place_order(price, "BUY", layer_idx=i, is_open=True)

        logger.info(f"🚀 网格已部署: {self.place_open_qty} 层待成交")

    # ---------------------------------------------------------------
    #  定时任务
    # ---------------------------------------------------------------
    def _record_snapshot(self):
        if self.cfg["dry_run"] or not self.ib:
            return
        ticker = self.ib.reqMktData(self.contract, "", False, False)
        self.ib.sleep(0.3)
        self.db.save_market_snapshot(
            self.cfg["symbol"],
            ticker.marketPrice(),
            ticker.bid,
            ticker.ask,
            int(ticker.volume or 0),
        )

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
        except Exception as e:
            logger.warning(f"⚠️ 权益快照失败: {e}")

    # ---------------------------------------------------------------
    #  主循环
    # ---------------------------------------------------------------
    def run_forever(self):
        """主事件循环"""
        self.running = True
        self.deploy()
        logger.info("⏳ 成交驱动网格运行中 (Ctrl+C 停止)...")

        snapshot_interval = self.cfg.get("snapshot_interval_s", 5)
        equity_interval   = self.cfg.get("equity_interval_s", 60)
        _last_snap = 0.0
        _last_eq   = 0.0

        while self.running:
            try:
                if self.ib and not self.cfg["dry_run"]:
                    self.ib.sleep(1)
                else:
                    time.sleep(1)

                t = time.time()
                if t - _last_snap >= snapshot_interval:
                    self._record_snapshot()
                    _last_snap = t
                if t - _last_eq >= equity_interval:
                    self._record_equity()
                    _last_eq = t

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.exception(f"💥 异常: {e}")
                time.sleep(5)

        self.stop()

    # ---------------------------------------------------------------
    #  生命周期
    # ---------------------------------------------------------------
    def initialize(self) -> "FillDrivenGrid":
        """连接 IBKR + DuckDB, 初始化策略"""
        cfg = self.cfg

        # DB
        self.db = Database(cfg["db_path"]).connect()
        # 先算层数用于 DB 记录
        _layer_count = cfg.get("max_position", 500) // cfg.get("qty_per_level", 100)
        low = cfg.get("base_price", 440) - _layer_count * cfg.get("buy_step_price", 2.5)
        high = cfg.get("base_price", 440) + _layer_count * cfg.get("sell_step_price", 2.5)
        db_cfg = {
            "name": cfg.get("name", "grid"),
            "symbol": cfg["symbol"],
            "exchange": cfg.get("exchange", "SMART"),
            "currency": cfg.get("currency", "USD"),
            "sec_type": cfg.get("sec_type", "STK"),
            "lower_price": round(low, 2),
            "upper_price": round(high, 2),
            "grid_levels": _layer_count,
            "qty_per_level": cfg["qty_per_level"],
            "max_position": cfg.get("max_position", 0),
            "status": "ACTIVE",
            "dry_run": cfg.get("dry_run", True),
        }
        self.config_id = self.db.save_config(db_cfg)

        # IBKR
        if not self.cfg["dry_run"]:
            self.ib = IB()
            self.ib.connect(
                self.cfg["ibkr_host"], self.cfg["ibkr_port"],
                clientId=self.cfg["ibkr_client_id"],
            )
            self.ib.errorEvent += self._on_error
            self.contract = Stock(
                self.cfg["symbol"], self.cfg["exchange"], self.cfg["currency"],
            )
            self.ib.qualifyContracts(self.contract)
            logger.info(f"✅ IBKR 已连接 — 账户: {self.ib.managedAccounts()}")
        else:
            self.ib = None
            self.contract = None
            logger.info("🧪 DRY RUN 模式")

        # 策略初始化 (核心)
        self.trigger_symbols()
        self.global_variables()

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        return self

    def stop(self):
        """停止策略"""
        logger.info("⏹ 停止网格策略...")
        self.running = False

        # 打印汇总
        self._print_summary()

        if self.db:
            self.db.update_config_status(self.config_id, "STOPPED")

        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
            logger.info("🔌 IBKR 已断开")

        if self.db:
            self.db.close()

    def _print_summary(self):
        """打印运行汇总"""
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
        logger.info(f"   当前持仓:   {self.策略当前持仓股数} 股")
        logger.info("=" * 52)

    # ---------------------------------------------------------------
    #  回调
    # ---------------------------------------------------------------
    def _on_error(self, _req_id, error_code, error_string, *_):
        logger.error(f"❌ IBKR Error (code={error_code}): {error_string}")

    def _handle_signal(self, signum, _frame):
        logger.info(f"📥 信号 {signum}, 退出...")
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
    "name": "SPY_grid",
    "base_price": 440.0,              # 初始基准价
    "price_mode": 0,                  # 0: 按价格  1: 按比例
    "buy_step_price": 2.5,            # 股价每跌多少买入
    "sell_step_price": 2.5,           # 股价每涨多少卖出
    "qty_per_level": 100,             # 每笔委托股数
    "max_position": 500,              # 策略最大持仓股数
    "precision": 10,                  # 价格精度 (小数位数)
    "dry_run": True,
    "snapshot_interval_s": 5,
    "equity_interval_s": 60,
    "db_path": "grid_trading.duckdb",
}


def run(config_override: dict | None = None, config_file: str | None = None):
    """运行网格策略"""
    cfg = dict(DEFAULT_CONFIG)
    if config_file:
        path = Path(config_file)
        if path.exists():
            with open(path) as f:
                cfg.update(json.load(f))
    if config_override:
        cfg.update(config_override)

    engine = FillDrivenGrid()
    engine.cfg = cfg
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
        "base_price": 440.0,          # ← 改为当前价附近的基准价
        "buy_step_price": 2.5,
        "sell_step_price": 2.5,
        "qty_per_level": 100,
        "max_position": 500,
        "dry_run": True,              # 先 dry-run 验证
    }
    # ===========================

    run(MY_CONFIG)
