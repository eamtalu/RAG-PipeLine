"""One guard per provider, shared by every run of the process: timeout, retry, breaker, bulkhead.

`ProviderGuard.call` runs one provider call: it takes a bulkhead slot, bounds the call by the
call timeout, retries once with jitter when the provider throttled, counts failures for the
breaker and frees the slot however the call ended. `availability` is what the router checks
before pinning: an open breaker or a full bulkhead keeps the provider out of the pick. The guard
is a counter rather than an `asyncio.Semaphore`, because a semaphore binds to the first event
loop that uses it and the test suite runs one loop per test; the loop is single-threaded, so a
counter changed between awaits is race-free. `ProviderHealth` holds the guards by provider name.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Awaitable, Callable, TypeVar

from app.agent.contracts import ResilienceSettings
from app.agent.router.failures import ModelCallTimeout, classify

logger = logging.getLogger(__name__)

T = TypeVar("T")
#: how long the bulkhead wait sleeps between looks at the counter
SLOT_POLL_SECONDS = 0.05
#: the retry waits this plus a jitter in [0, 1) seconds
RETRY_BASE_SECONDS = 0.5


class ProviderGuard:
    """The guard of one provider; `state` is closed, open or half_open."""

    def __init__(
        self,
        provider: str,
        resilience: ResilienceSettings,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ):
        self.provider = provider
        self.resilience = resilience
        self._clock, self._sleep, self._jitter = clock, sleep, jitter
        self.in_flight = 0
        self.failures = 0  # consecutive, reset by a success
        self.opened_at: float | None = None
        self.calls_made = 0

    @property
    def state(self) -> str:
        """closed while the provider answers; open after too many failures; half_open after the
        cool-down, when one call may probe it."""
        if self.opened_at is None:
            return "closed"
        elapsed = self._clock() - self.opened_at
        return "open" if elapsed < self.resilience.breaker_cooldown_seconds else "half_open"

    def availability(self) -> str | None:
        """None when the router may pin this provider, else why not.

        (closed, 2 of 8 in flight) -> None ; (open) -> "breaker open" ; (8 of 8) -> "bulkhead full"
        """
        state = self.state
        if state == "open":
            return "breaker open"
        if state == "half_open" and self.in_flight > 0:
            return "breaker half-open, a probe is in flight"
        if self.in_flight >= self.resilience.bulkhead:
            return "bulkhead full"
        return None

    async def call(self, provider_call: Callable[[], Awaitable[T]], *, timeout_seconds: float) -> T:
        """Run one call under the guard; the result, or the failure after the policy is spent.

        (a call that answers) -> its result ; (throttled once, then answers) -> its result after
        one jittered wait ; (a 500) -> the error at once ; (hangs) -> ModelCallTimeout
        """
        # Step 1: Take a slot, waiting within the call timeout, so that a slow provider holds at
        # most `bulkhead` of this process's calls and a pinned run still gets its turn.
        give_up_at = self._clock() + timeout_seconds
        while self.in_flight >= self.resilience.bulkhead:
            if self._clock() >= give_up_at:
                raise ModelCallTimeout(
                    self.provider, timeout_seconds, "waiting for a bulkhead slot"
                )
            await self._sleep(SLOT_POLL_SECONDS)
        self.in_flight += 1
        try:
            attempt = 1
            while True:
                self.calls_made += 1
                try:
                    # Step 2: Bound the call, because a provider that never answers must cost one
                    # failed call, not the run's whole deadline.
                    result = await asyncio.wait_for(provider_call(), timeout=timeout_seconds)
                except asyncio.TimeoutError:
                    error = ModelCallTimeout(self.provider, timeout_seconds)
                    self._record_failure("timeout", error)
                    raise error from None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    kind = classify(exc)
                    self._record_failure(kind, exc)
                    # Step 3: Retry once only when the provider throttled, with jitter, because
                    # a 429 means "later" and every other failure means "not here".
                    if kind == "throttled" and attempt == 1:
                        wait = RETRY_BASE_SECONDS + self._jitter()
                        logger.warning(
                            "agent.model.retry provider=%s wait=%.2f", self.provider, wait
                        )
                        await self._sleep(wait)
                        attempt += 1
                        continue
                    raise
                self._record_success()
                return result
        finally:
            # Step 4: Free the slot however the call ended, so a failure can never pin the
            # bulkhead.
            self.in_flight -= 1

    def _record_failure(self, kind: str, error: BaseException) -> None:
        self.failures += 1
        logger.warning(
            "agent.model.failed provider=%s kind=%s error=%s: %s",
            self.provider,
            kind,
            type(error).__name__,
            str(error)[:200],
        )
        if self.failures >= self.resilience.breaker_failures or self.opened_at is not None:
            was = self.state
            self.opened_at = self._clock()
            if was != "open":
                logger.warning("agent.breaker provider=%s state=open", self.provider)

    def _record_success(self) -> None:
        if self.opened_at is not None:
            logger.info("agent.breaker provider=%s state=closed", self.provider)
        self.failures, self.opened_at = 0, None


class ProviderHealth:
    """The guards of this process, one per provider, made on first use with the run's numbers."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ):
        self._clock, self._sleep, self._jitter = clock, sleep, jitter
        self._guards: dict[str, ProviderGuard] = {}

    def guard_for(self, provider: str, resilience: ResilienceSettings) -> ProviderGuard:
        """The provider's guard; the first call for a provider fixes its numbers.

        ("bedrock", r) -> ProviderGuard("bedrock") ; ("bedrock", r) again -> the same object
        """
        guard = self._guards.get(provider)
        if guard is None:
            guard = ProviderGuard(
                provider, resilience, clock=self._clock, sleep=self._sleep, jitter=self._jitter
            )
            self._guards[provider] = guard
        return guard
