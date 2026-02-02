from __future__ import annotations

from dataclasses import dataclass
import asyncio
import logging
from typing import Any, Callable, Optional

from aiohttp_rpc import WsJsonRpcClient
from midas.base.feed import DataFeed, OnMessage, Option
from midas.helpers.datetime import from_ms
from midas.types.ticker import create_ticker

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 8.0


class DeribitError(Exception):
    """Base error for Deribit feed failures."""


class TransientDeribitError(DeribitError):
    """Recoverable Deribit error."""


class PermanentDeribitError(DeribitError):
    """Non-recoverable Deribit error."""


class CriticalDeribitError(DeribitError):
    """Critical error that should stop the bot."""


class FeedMode:
    NORMAL = "normal"
    RETRYING = "retrying"
    READ_ONLY = "read_only"
    IDLE_SAFE = "idle_safe"


class DeribitFeed(DataFeed):
    def __init__(self, ws_url: str, retry_policy: Optional[RetryPolicy] = None):
        """Use `DeribitFeed.create` to create an instance of this class."""
        assert ws_url, "ws_url is required"
        self._ws = WsJsonRpcClient(ws_url, json_request_handler=self._on_request)
        self._retry_policy = retry_policy or RetryPolicy()
        self._mode = FeedMode.NORMAL
        self._on_message: Optional[OnMessage] = None


    @staticmethod
    async def create(ws_url: str, retry_policy: Optional[RetryPolicy] = None):
        feed = DeribitFeed(ws_url, retry_policy=retry_policy)
        await feed._connect_with_retry()
        return feed


    async def get_options(self, currency: str, expired: bool = False):
        assert currency, "currency is required"
        if self._mode == FeedMode.IDLE_SAFE:
            logger.info("deribit.feed.get_options.skipped", extra={"mode": self._mode})
            return []
        raw_options = await self._call_with_retry(
            "public/get_instruments",
            currency=currency,
            expired=expired,
            kind="option",
        )
        if raw_options is None:
            return []
        self._assert_response_is_list(raw_options, "get_instruments")
        all_options = [create_option(item) for item in raw_options]
        return sorted(all_options, key=lambda option: (option.expiration, option.strike))


    async def subscribe(self, channels: list[str], on_message: OnMessage):
        assert channels, "channels are required"
        assert callable(on_message), "on_message must be callable"
        if self._mode == FeedMode.IDLE_SAFE:
            logger.info("deribit.feed.subscribe.skipped", extra={"mode": self._mode})
            return
        self._on_message = on_message
        await self._call_with_retry("public/subscribe", channels=channels)


    async def _on_request(self, json_request: dict[str, Any], **kwargs: list[Any]):
        if not self._on_message:
            logger.warning("deribit.feed.message.dropped", extra={"reason": "no_subscriber"})
            return
        try:
            self._on_message(json_request)
        except Exception as exc:
            logger.exception(
                "deribit.feed.message.handler.failed",
                extra={"error": str(exc)},
            )


    async def get_ticker(self, instrument: str):
        assert instrument, "instrument is required"
        if self._mode == FeedMode.IDLE_SAFE:
            logger.info("deribit.feed.get_ticker.skipped", extra={"mode": self._mode})
            return None
        ticker = await self._call_with_retry("public/ticker", instrument_name=instrument)
        if ticker is None:
            return None
        self._assert_response_is_dict(ticker, "ticker")
        return create_ticker(ticker)


    async def close(self):
        await self._ws.close()


    async def _connect_with_retry(self):
        await self._run_with_retry(self._ws.connect, action="connect")


    async def _call_with_retry(self, method: str, **kwargs: Any):
        return await self._run_with_retry(
            lambda: self._ws.call(method, **kwargs),
            action=method,
        )


    async def _run_with_retry(self, func: Callable[[], Any], action: str):
        attempts = self._retry_policy.max_attempts
        assert attempts > 0, "retry attempts must be positive"
        backoff = self._retry_policy.base_backoff_seconds
        for attempt in range(1, attempts + 1):
            try:
                if attempt > 1:
                    self._set_mode(FeedMode.RETRYING)
                result = await func()
                if self._mode != FeedMode.NORMAL:
                    self._set_mode(FeedMode.NORMAL)
                return result
            except AssertionError as exc:
                self._set_mode(FeedMode.IDLE_SAFE)
                logger.exception(
                    "deribit.feed.assertion_failed",
                    extra={"action": action, "error": str(exc)},
                )
                raise CriticalDeribitError(str(exc)) from exc
            except CriticalDeribitError:
                self._set_mode(FeedMode.IDLE_SAFE)
                logger.exception("deribit.feed.critical_error", extra={"action": action})
                raise
            except PermanentDeribitError as exc:
                self._set_mode(FeedMode.READ_ONLY)
                logger.exception(
                    "deribit.feed.permanent_error",
                    extra={"action": action, "error": str(exc)},
                )
                return None
            except Exception as exc:
                if not self._is_transient_error(exc):
                    self._set_mode(FeedMode.READ_ONLY)
                    logger.exception(
                        "deribit.feed.unexpected_error",
                        extra={"action": action, "error": str(exc)},
                    )
                    return None
                logger.warning(
                    "deribit.feed.retrying",
                    extra={"action": action, "attempt": attempt, "error": str(exc)},
                )
                if attempt == attempts:
                    self._set_mode(FeedMode.READ_ONLY)
                    logger.error(
                        "deribit.feed.retry_exhausted",
                        extra={"action": action, "attempts": attempts},
                    )
                    return None
                sleep_for = min(backoff, self._retry_policy.max_backoff_seconds)
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, self._retry_policy.max_backoff_seconds)
        return None


    def _set_mode(self, mode: str):
        if self._mode == mode:
            return
        logger.info("deribit.feed.mode_changed", extra={"from": self._mode, "to": mode})
        self._mode = mode


    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        return isinstance(exc, (TransientDeribitError, asyncio.TimeoutError, OSError))


    @staticmethod
    def _assert_response_is_list(value: Any, action: str):
        assert isinstance(value, list), f"Expected list response for {action}"


    @staticmethod
    def _assert_response_is_dict(value: Any, action: str):
        assert isinstance(value, dict), f"Expected dict response for {action}"


def create_option(item: dict[str, Any]):
    assert isinstance(item, dict), "option payload must be a dict"
    required = (
        "instrument_name",
        "creation_timestamp",
        "expiration_timestamp",
        "strike",
        "option_type",
    )
    missing = [key for key in required if key not in item]
    if missing:
        raise PermanentDeribitError(f"Missing option fields: {missing}")
    return Option(
        name=item["instrument_name"],
        creation=from_ms(item["creation_timestamp"]),
        expiration=from_ms(item["expiration_timestamp"]),
        strike=int(item["strike"]),
        type=item["option_type"],
    )
