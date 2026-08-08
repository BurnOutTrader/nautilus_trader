#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import os
import signal
import threading
import time
from collections.abc import Callable


def interrupt_live_node_run() -> None:
    os.kill(os.getpid(), signal.SIGINT)


def schedule_live_node_interrupt(run_seconds: float) -> threading.Timer | None:
    if run_seconds <= 0:
        return None

    timer = threading.Timer(run_seconds, interrupt_live_node_run)
    timer.daemon = True
    timer.start()
    return timer


def start_live_node_monitor(
    target: Callable[[], None],
    *,
    name: str,
) -> threading.Thread:
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def wait_for_event_or_cancel(
    waiter: Callable[[float], bool],
    timeout: float,
    *,
    cancel: threading.Event,
    poll_seconds: float = 0.1,
) -> bool:
    deadline = time.monotonic() + max(timeout, 0.0)

    while True:
        if cancel.is_set():
            return False

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return waiter(0.0)

        if waiter(min(poll_seconds, remaining)):
            return True


def sleep_or_cancel(
    duration_seconds: float,
    *,
    cancel: threading.Event,
    poll_seconds: float = 0.1,
) -> bool:
    deadline = time.monotonic() + max(duration_seconds, 0.0)

    while True:
        if cancel.is_set():
            return False

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True

        time.sleep(min(poll_seconds, remaining))
