"""Per-device coordinator that broadcasts data changes to HA entities.

Each BaseDevice creates one instance. On each poll cycle it checks whether the
underlying EcoflowDataHolder received new data since the last broadcast and
notifies listening entities accordingly.
"""

from __future__ import annotations

import dataclasses
import datetime
import logging
from datetime import timedelta

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt

from ..api import EcoflowApiClient
from .data_holder import EcoflowDataHolder
from .status_tracker import StatusTracker

_LOGGER = logging.getLogger(__name__)
AUTO_RECONNECT_TRIGGER_SEC = 60


@dataclasses.dataclass
class EcoflowBroadcastDataHolder:
    data_holder: EcoflowDataHolder
    changed: bool


class DeviceDataCoordinator(DataUpdateCoordinator[EcoflowBroadcastDataHolder]):
    def __init__(
        self,
        hass: HomeAssistant,
        client: EcoflowApiClient,
        holder: EcoflowDataHolder,
        status_tracker: StatusTracker,
        refresh_period: int,
        assume_offline_sec: int,
        device_sn: str,
        device_name: str,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name="Ecoflow update coordinator",
            always_update=True,
            update_interval=datetime.timedelta(seconds=max(refresh_period, 5)),
        )
        self.client = client
        self.holder = holder
        self._status_tracker = status_tracker
        self._assume_offline_sec = assume_offline_sec
        self._device_sn = device_sn
        self._device_name = device_name
        self.__last_broadcast = dt.utcnow().replace(year=2000, month=1, day=1, hour=0, minute=0, second=0)

    async def _async_check_mqtt_watchdog(self) -> None:
        mqtt_client = getattr(self.client, "mqtt_client", None)
        if mqtt_client is None:
            return

        now = dt.utcnow()
        connect_time = mqtt_client.last_connect_time
        if connect_time is None:
            return

        refresh_sec = int(self.update_interval / timedelta(seconds=1))
        startup_grace_sec = max(30, refresh_sec * 3)
        if (now - connect_time).total_seconds() < startup_grace_sec:
            return

        device_data_age_sec = (now - self._status_tracker.last_auto_data_time).total_seconds()
        connect_age_sec = (now - connect_time).total_seconds()
        mqtt_data_age_sec = (now - mqtt_client.last_message_time).total_seconds() if mqtt_client.has_seen_message else connect_age_sec
        silence_threshold_sec = AUTO_RECONNECT_TRIGGER_SEC

        mqtt_disconnected = not mqtt_client.is_connected()
        mqtt_stalled = mqtt_data_age_sec >= silence_threshold_sec
        device_stalled = device_data_age_sec >= silence_threshold_sec

        should_reconnect = device_stalled and (mqtt_disconnected or mqtt_stalled)
        if not should_reconnect:
            return

        reason = (
            f"mqtt unhealthy (connected={not mqtt_disconnected}, mqtt_silence={int(mqtt_data_age_sec)}s, "
            f"device_silence={int(device_data_age_sec)}s, threshold={silence_threshold_sec}s, "
            f"device={self._device_name}/{self._device_sn})"
        )
        did_reconnect = await self.client.async_reconnect(reason, min_interval_sec=AUTO_RECONNECT_TRIGGER_SEC)
        if did_reconnect:
            _LOGGER.warning("Auto MQTT reconnect succeeded for %s (%s)", self._device_name, self._device_sn)

    async def _async_update_data(self) -> EcoflowBroadcastDataHolder:
        received_time = self.holder.last_received_time()
        changed = self.__last_broadcast < received_time
        self.__last_broadcast = received_time
        await self._async_check_mqtt_watchdog()
        return EcoflowBroadcastDataHolder(self.holder, changed)
