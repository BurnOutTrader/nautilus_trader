#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2026 Kevin Monaghan. All rights reserved.
#
#  Licensed under the GNU Lesser General Public License Version 3.0 or later.
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

"""
Example: connect to the Rithmic test route and stay connected.

This example is intended for the simple Rithmic API conformance workflow where
Rithmic asks you to connect to the test endpoint and keep the session alive.

This example:
1. Reads the Rithmic username and password from the environment
2. Forces the Rithmic TEST environment and the named `Test` server route
3. Connects the test session through `RithmicGateway`
4. Idles until you stop it manually with `Ctrl+C`

Required user inputs:
    RITHMIC_USERNAME
    RITHMIC_PASSWORD

Optional user inputs:
    RITHMIC_ALT_SERVER

Example configuration:
    Edit `POLL_INTERVAL_SECONDS` below if you want a different heartbeat check interval.

Operator note:
    - This script is intentionally minimal for the username/password-only
      conformance flow.
    - It supplies the adapter-required non-user fields internally so the user
      does not need to provide them for this test-connect workflow.

Notes:
    - In NautilusTrader, the vendor "test URL" maps to
      `RithmicEnv.TEST` plus the named `Test` server route.
    - This script assumes the current conformance requirement is only to
      connect and stay connected.
    - If Rithmic changes its conformance requirements, the user would need to
      code a custom strategy or workflow to satisfy those new requirements.
    - This script connects only the ticker plant and does not subscribe to
      market data or submit orders. It exists only to prove connectivity and
      keep the session open.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal

from nautilus_trader.adapters.rithmic import RithmicEnv
from nautilus_trader.adapters.rithmic import RithmicGateway
from nautilus_trader.adapters.rithmic import load_rithmic_env_file


DEFAULT_APP_NAME = "NautilusRithmicConformance"
DEFAULT_APP_VERSION = "1.0"
POLL_INTERVAL_SECONDS = 5.0

load_rithmic_env_file()


def required_env(key: str) -> str:
    value = os.getenv(key)

    if value:
        return value
    raise ValueError(f"{key} environment variable not set")


def build_gateway() -> RithmicGateway:
    return RithmicGateway(
        environment=RithmicEnv.TEST,
        username=required_env("RITHMIC_USERNAME"),
        password=required_env("RITHMIC_PASSWORD"),
        system_name="",
        app_name=DEFAULT_APP_NAME,
        app_version=DEFAULT_APP_VERSION,
        fcm_id="",
        ib_id="",
        account_id="",
        server="Test",
        alt_server=os.getenv("RITHMIC_ALT_SERVER"),
        enable_ticker=True,
        enable_order=False,
        enable_pnl=False,
        enable_history=False,
    )


async def keepalive() -> None:
    alt_server = os.getenv("RITHMIC_ALT_SERVER")
    poll_interval = float(POLL_INTERVAL_SECONDS)

    if poll_interval <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be positive")

    gateway = build_gateway()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    print("Rithmic Conformance Keepalive")
    print("=" * 50)
    print("Environment: TEST")
    print("Primary server: Test")
    print(f"Alternate server: {alt_server or '<none>'}")
    print("Plants: ticker")
    print("Press Ctrl+C to disconnect.")
    print()

    await gateway.connect()
    print(f"Connected. state={gateway.connection_state()}")

    last_state = gateway.connection_state()

    try:
        while not stop_event.is_set():
            state = gateway.connection_state()
            connected = gateway.is_connected()

            if state != last_state:
                print(f"Connection state changed: {last_state} -> {state}")
                last_state = state
            elif not connected:
                print(f"Gateway not yet fully connected. state={state}")

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
            except TimeoutError:
                continue
    finally:
        print("Disconnecting...")
        await gateway.disconnect()
        print(f"Disconnected. state={gateway.connection_state()}")


def main() -> None:
    asyncio.run(keepalive())


if __name__ == "__main__":
    main()
