"""
Dashboard web server — serves the evaluation UI on a dedicated port.

Runs separately from the authenticated validator API so testnet operators
can open the dashboard without configuring MASTER_KEY.
"""

import time
from threading import Thread
from typing import Optional

import bittensor as bt
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from vali_utils.dashboard.routes import router
from vali_utils.dashboard.scheduler import OnDemandJobScheduler
from vali_utils.dashboard.settings import DashboardSettings, get_settings_manager


class DashboardServer:
    """FastAPI server for the validator evaluation dashboard."""

    def __init__(self, validator, port: int = 8080, persist_settings: bool = True):
        self.validator = validator
        self.port = port
        settings_path = None
        if persist_settings and hasattr(validator, "config"):
            settings_path = (
                f"{validator.config.neuron.full_path}/dashboard_settings.json"
            )
        self.settings_manager = get_settings_manager(settings_path)
        self.od_scheduler = OnDemandJobScheduler(self.settings_manager)
        self.app = self._create_app()
        self._thread: Optional[Thread] = None

    def _create_app(self) -> FastAPI:
        app = FastAPI(
            title="Data Universe Validator Dashboard",
            description="Real-time miner evaluation monitoring for testnet",
            version="1.0.0",
        )
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )
        app.state.validator = self.validator
        app.state.od_scheduler = self.od_scheduler
        app.include_router(router, prefix="/dashboard")
        return app

    def start(self):
        if self._thread and self._thread.is_alive():
            bt.logging.warning("Dashboard server already running")
            return

        self.od_scheduler.start()

        # Resume scheduler if auto OD was persisted as enabled.
        settings = self.settings_manager.get()
        if self.od_scheduler.is_enabled(settings):
            disabled = DashboardSettings(auto_od_enabled=False)
            self.od_scheduler.sync_after_settings_save(disabled, settings)

        def _run():
            try:
                bt.logging.info(f"Starting dashboard on port {self.port}")
                uvicorn.run(
                    self.app, host="0.0.0.0", port=self.port, log_level="warning"
                )
            except Exception as e:
                bt.logging.error(f"Dashboard server error: {e}")

        self._thread = Thread(target=_run, daemon=True, name="dashboard-server")
        self._thread.start()
        bt.logging.success(
            f"Dashboard available at http://localhost:{self.port}/dashboard/"
        )

    def stop(self):
        self.od_scheduler.stop()
        if self._thread:
            self._thread.join(timeout=3)
