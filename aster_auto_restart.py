"""
Aster 自动重启封装
==================

简介
----
本脚本在不修改原有 TradingBot 代码的前提下，为 Aster 交易所提供自动重启能力。它会在运行
过程中监听异常、停机条件与系统信号，确保 TradingBot 掉线或触发停止后能够按设定策略重新
拉起，降低人工干预频率。

使用方法
--------
1. 按 `env_example.txt` 准备好包含 `ASTER_API_KEY`、`ASTER_SECRET_KEY` 等配置的 `.env` 文件。
2. 安装项目依赖（至少需要 `python-dotenv`、`tenacity`、`aiohttp`、`websockets` 等）。
3. 在项目根目录执行：
   `python aster_auto_restart.py --exchange aster --env-file your.env --ticker ETH --quantity 0.1 --take-profit 0.02`
   可选参数：
   - `--max-restarts` / `--restart-delay` / `--restart-delay-max` / `--backoff-multiplier` 控制重启策略；
   - `--restart-on-success` 让脚本在正常退出后继续轮询；
   - `--stop-price`、`--pause-price` 等 TradingBot 原生参数同样适用。
4. 观察终端日志确认是否出现 `Starting trading cycle`、`Waiting … seconds before restart` 等提示。
5. 使用 `Ctrl+C` 可手动退出，脚本会捕获信号并执行优雅停机。
"""

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Callable

import dotenv

from exchanges import ExchangeFactory
from trading_bot import TradingBot, TradingConfig


@dataclass
class RestartPolicy:
    restart_on_success: bool
    max_restarts: int
    initial_delay: float
    max_delay: float
    backoff_multiplier: float


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments for the auto-restart helper."""
    parser = argparse.ArgumentParser(
        description="Auto-restart wrapper for the Aster trading bot"
    )

    parser.add_argument('--exchange', type=str, default='aster',
                        choices=ExchangeFactory.get_supported_exchanges(),
                        help='Exchange to trade on (must be aster).')
    parser.add_argument('--ticker', type=str, default='ETH',
                        help='Ticker symbol, e.g., ETH.')
    parser.add_argument('--quantity', type=Decimal, default=Decimal('0.1'),
                        help='Order quantity per cycle.')
    parser.add_argument('--take-profit', type=Decimal, default=Decimal('0.02'),
                        help='Take profit percentage (e.g., 0.02 = 0.02%).')
    parser.add_argument('--direction', type=str, default='buy', choices=['buy', 'sell'],
                        help='Trading direction (buy or sell).')
    parser.add_argument('--max-orders', type=int, default=40,
                        help='Maximum number of concurrent close orders.')
    parser.add_argument('--wait-time', type=int, default=450,
                        help='Base wait time between open orders in seconds.')
    parser.add_argument('--grid-step', type=Decimal, default=Decimal('-100'),
                        help='Minimum spacing (percentage) between close orders.')
    parser.add_argument('--stop-price', type=Decimal, default=Decimal('-1'),
                        help='Stop trading if price crosses this threshold (-1 disables).')
    parser.add_argument('--pause-price', type=Decimal, default=Decimal('-1'),
                        help='Pause trading if price crosses this threshold (-1 disables).')
    parser.add_argument('--env-file', type=str, default='.env',
                        help='Path to environment file with API credentials.')
    parser.add_argument('--boost', action='store_true',
                        help='Enable boost mode (maker + taker cycling).')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='Console log level (DEBUG, INFO, WARNING, ERROR).')

    # Restart policy tuning
    parser.add_argument('--max-restarts', type=int, default=0,
                        help='Maximum restart attempts (0 = unlimited).')
    parser.add_argument('--restart-on-success', action='store_true',
                        help='Restart even after a clean shutdown.')
    parser.add_argument('--restart-delay', type=float, default=30.0,
                        help='Initial delay between restarts in seconds.')
    parser.add_argument('--restart-delay-max', type=float, default=300.0,
                        help='Maximum delay between restarts in seconds.')
    parser.add_argument('--backoff-multiplier', type=float, default=2.0,
                        help='Multiplier applied to the delay after each restart (>= 1.0).')

    return parser.parse_args()


def configure_logging(log_level: str) -> None:
    """Configure root logging for the helper."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format='[ASTER_RESTART] %(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def validate_arguments(args: argparse.Namespace) -> None:
    """Run guard checks on parsed arguments."""
    if args.exchange.lower() != 'aster':
        raise SystemExit('This helper only supports --exchange aster.')

    if args.boost is False:
        return

    # Boost mode is valid for aster, so no additional validation required.


def build_config_factory(args: argparse.Namespace) -> Callable[[], TradingConfig]:
    """Create a factory that returns fresh TradingConfig instances for each restart."""
    base_kwargs = {
        'ticker': args.ticker.upper(),
        'contract_id': '',
        'quantity': args.quantity,
        'take_profit': args.take_profit,
        'tick_size': Decimal('0'),
        'direction': args.direction.lower(),
        'max_orders': args.max_orders,
        'wait_time': args.wait_time,
        'exchange': args.exchange.lower(),
        'grid_step': args.grid_step,
        'stop_price': args.stop_price,
        'pause_price': args.pause_price,
        'boost_mode': args.boost,
    }

    def factory() -> TradingConfig:
        return TradingConfig(**base_kwargs)

    return factory


async def run_with_auto_restart(
    config_factory: Callable[[], TradingConfig],
    restart_policy: RestartPolicy,
    stop_event: asyncio.Event,
) -> None:
    """Run TradingBot with restart semantics until stop_event is set."""
    attempt = 0
    delay = max(0.0, restart_policy.initial_delay)
    delay = min(delay, restart_policy.max_delay) if restart_policy.max_delay > 0 else delay

    while not stop_event.is_set():
        attempt += 1
        config = config_factory()
        bot = TradingBot(config)
        logging.info('Starting trading cycle #%d', attempt)

        run_succeeded = False
        try:
            await bot.run()
            run_succeeded = True
            logging.info('Trading cycle #%d completed gracefully.', attempt)
        except asyncio.CancelledError:
            logging.warning('Trading cycle #%d cancelled.', attempt)
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logging.exception('Trading cycle #%d exited with error: %s', attempt, exc)
        finally:
            try:
                await bot.graceful_shutdown('Restart loop cleanup')
            except Exception as exc:  # pylint: disable=broad-except
                logging.exception('Error during graceful shutdown: %s', exc)

        if stop_event.is_set():
            logging.info('Stop event received; exiting restart loop.')
            break

        if run_succeeded and not restart_policy.restart_on_success:
            logging.info('Stopping after successful run (restart-on-success disabled).')
            break

        if restart_policy.max_restarts and attempt >= restart_policy.max_restarts:
            logging.info('Reached max restart limit (%d); exiting.', restart_policy.max_restarts)
            break

        if restart_policy.max_delay > 0:
            delay = min(delay, restart_policy.max_delay)

        logging.info('Waiting %.1f seconds before restart.', delay)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
            logging.info('Stop event received during cooldown; exiting.')
            break
        except asyncio.TimeoutError:
            pass

        if restart_policy.backoff_multiplier >= 1.0:
            delay = delay * restart_policy.backoff_multiplier
            if restart_policy.max_delay > 0:
                delay = min(delay, restart_policy.max_delay)


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    """Register signal handlers that set the stop_event."""
    def _handler(signum, _frame):  # type: ignore[override]
        logging.warning('Received signal %s; scheduling shutdown.', signum)
        stop_event.set()

    for sig_name in ('SIGINT', 'SIGTERM'):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            signal.signal(sig, _handler)


async def async_main() -> None:
    args = parse_arguments()
    configure_logging(args.log_level)
    validate_arguments(args)

    env_path = Path(args.env_file)
    if not env_path.exists():
        raise SystemExit(f'Env file not found: {env_path.resolve()}')

    dotenv.load_dotenv(args.env_file)

    restart_policy = RestartPolicy(
        restart_on_success=args.restart_on_success,
        max_restarts=args.max_restarts,
        initial_delay=args.restart_delay,
        max_delay=args.restart_delay_max,
        backoff_multiplier=max(args.backoff_multiplier, 1.0),
    )

    config_factory = build_config_factory(args)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    await run_with_auto_restart(config_factory, restart_policy, stop_event)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logging.warning('Interrupted by user.')


if __name__ == '__main__':
    main()
