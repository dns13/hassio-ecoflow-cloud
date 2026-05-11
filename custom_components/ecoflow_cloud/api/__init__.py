import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import asyncio

from aiohttp import ClientResponse
from attr import dataclass

from ..device_data import DeviceData
from .message import JSONMessage, Message

_LOGGER = logging.getLogger(__name__)


class EcoflowException(Exception):
    pass


@dataclass
class EcoflowMqttInfo:
    url: str
    port: int
    username: str
    password: str
    client_id: str | None = None


class EcoflowApiClient(ABC):
    def __init__(self):
        from custom_components.ecoflow_cloud.api.ecoflow_mqtt import EcoflowMQTTClient

        self.mqtt_info: EcoflowMqttInfo
        self.devices: dict[str, Any] = {}
        self.mqtt_client: EcoflowMQTTClient
        self._reconnect_lock = asyncio.Lock()
        self._last_reconnect_monotonic = 0.0
        self._reconnect_count = 0

    @abstractmethod
    async def login(self):
        pass

    @abstractmethod
    async def fetch_all_available_devices(self):
        pass

    @abstractmethod
    async def quota_all(self, device_sn: str | None):
        pass

    @abstractmethod
    def _create_device_info(
        self, device_sn: str, device_name: str, device_type: str, status: int = -1
    ) -> Any:
        pass

    @abstractmethod
    def _device_registry(self) -> dict[str, Any]:
        pass

    def configure_device(self, device_data: DeviceData, api_devices_info: dict[str, Any] | None = None):
        sn = device_data.parent.sn if device_data.parent is not None else device_data.sn
        status = -1
        if api_devices_info and sn in api_devices_info:
            status = api_devices_info[sn].status

        if device_data.parent is not None:
            info = self._create_device_info(device_data.parent.sn, device_data.name, device_data.parent.device_type, status)
        else:
            info = self._create_device_info(device_data.sn, device_data.name, device_data.device_type, status)

        from ..devices import DiagnosticDevice

        registry = self._device_registry()
        if device_data.device_type in registry:
            device = registry[device_data.device_type](info, device_data)
        elif device_data.parent is not None and device_data.parent.device_type in registry:
            device = registry[device_data.parent.device_type](info, device_data)
        else:
            device = DiagnosticDevice(info, device_data)

        self.add_device(device)
        return device

    def add_device(self, device):
        self.devices[device.device_data.sn] = device

    def remove_device(self, device):
        self.devices.pop(device.device_data.sn, None)

    def _accept_mqqt_certification(self, resp_json: dict):
        _LOGGER.info(f"Received MQTT credentials: {resp_json}")
        try:
            mqtt_url = resp_json["data"]["url"]
            mqtt_port = int(resp_json["data"]["port"])
            mqtt_username = resp_json["data"]["certificateAccount"]
            mqtt_password = resp_json["data"]["certificatePassword"]
            self.mqtt_info = EcoflowMqttInfo(mqtt_url, mqtt_port, mqtt_username, mqtt_password)
        except KeyError as key:
            raise EcoflowException(f"Failed to extract key {key} from {resp_json}")

        _LOGGER.info(f"Successfully extracted account: {self.mqtt_info.username}")

    async def _get_json_response(self, resp: ClientResponse) -> dict[str, Any]:
        if resp.status != 200:
            raise EcoflowException(f"Got HTTP status code {resp.status}: {resp.reason}")

        try:
            json_resp = await resp.json()
            response_message = json_resp["message"]
        except KeyError as key:
            raise EcoflowException(f"Failed to extract key {key} from {resp}")
        except Exception as error:
            raise EcoflowException(f"Failed to parse response: {resp.text} Error: {error}")

        if response_message.lower() != "success":
            raise EcoflowException(f"{response_message}")

        return json_resp

    def send_get_message(self, device_sn: str, command: dict | Message):
        if isinstance(command, dict):
            command = JSONMessage(command)

        self.mqtt_client.publish(self.devices[device_sn].device_info.get_topic, command.to_mqtt_payload())

    def send_set_message(self, device_sn: str, mqtt_state: dict[str, Any], command: dict | Message):
        if isinstance(command, dict):
            command = JSONMessage(command)

        self.devices[device_sn].data.update_to_target_state(mqtt_state)
        self.mqtt_client.publish(self.devices[device_sn].device_info.set_topic, command.to_mqtt_payload())

    def start(self):
        _LOGGER.debug("Starting MQTT client for %s", self.mqtt_info.client_id)
        from custom_components.ecoflow_cloud.api.ecoflow_mqtt import EcoflowMQTTClient

        self.mqtt_client = EcoflowMQTTClient(self.mqtt_info, self.devices)

    def stop(self):
        _LOGGER.debug("Stopping MQTT client for %s", self.mqtt_info.client_id)
        assert self.mqtt_client is not None
        self.mqtt_client.stop()

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    async def async_reconnect(self, reason: str, min_interval_sec: int = 120, force: bool = False) -> bool:
        """Reconnect MQTT by re-authenticating and starting a new client session."""
        if self._reconnect_lock.locked() and not force:
            _LOGGER.debug("Reconnect already in progress for %s", self.mqtt_info.client_id)
            return False

        async with self._reconnect_lock:
            now = time.monotonic()
            if not force and (now - self._last_reconnect_monotonic) < min_interval_sec:
                _LOGGER.debug(
                    "Reconnect suppressed by cooldown for %s (reason=%s)",
                    self.mqtt_info.client_id,
                    reason,
                )
                return False

            if not force:
                _LOGGER.warning("Triggering MQTT reconnect for %s: %s", self.mqtt_info.client_id, reason)
            else:
                _LOGGER.debug("Triggering MQTT reconnect for %s: %s", self.mqtt_info.client_id, reason)
            try:
                try:
                    await asyncio.to_thread(self.stop)
                except Exception:
                    _LOGGER.debug("Ignoring stop() failure before reconnect", exc_info=True)

                await self.login()
                await asyncio.to_thread(self.start)
            except Exception as err:
                _LOGGER.error("MQTT reconnect failed for %s: %s", self.mqtt_info.client_id, err, exc_info=True)
                return False

            self._last_reconnect_monotonic = time.monotonic()
            self._reconnect_count += 1
            return True
