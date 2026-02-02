import asyncio
import logging
from pathlib import Path

import toml
from midas.live.deribit_feed import CriticalDeribitError, DeribitFeed
from midas.live.live_timer import LiveTimer

from .strategy.short_put import ShortPut

logger = logging.getLogger(__name__)


async def trade():
    config_path = Path(__file__).with_name('.env.toml')
    config = toml.load(config_path)

    feed = await DeribitFeed.create(config['broker']['ws_url'])
    timer = LiveTimer()

    strategy = ShortPut(feed, timer)
    await strategy.on_start()


def main():
    try:
        asyncio.get_event_loop().run_until_complete(trade())
        asyncio.get_event_loop().run_forever()
    except CriticalDeribitError:
        logger.exception("trade.critical_error")
    except KeyboardInterrupt:
        print('Interrupted..')
    except Exception as exc:
        logger.exception("trade.noncritical_error", extra={"error": str(exc)})
        asyncio.get_event_loop().run_forever()
