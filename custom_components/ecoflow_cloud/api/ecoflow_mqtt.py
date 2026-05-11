from __future__ import annotations

import logging
import ssl
from datetime import datetime
from typing import Any

from homeassistant.core import callback
from homeassistant.util import dt
from paho.mqtt.client import Client, ConnectFlags, DisconnectFlags, MQTTMessage, PayloadType
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from ..devices import BaseDevice
from . import EcoflowMqttInfo

_LOGGER = logging.getLogger(__name__)
_EPOCH = dt.utcnow().replace(year=2000, month=1, day=1, hour=0, minute=0, second=0)


class EcoflowMQTTClient:
    def __init__(self, mqtt_info: EcoflowMqttInfo, devices: dict[str, BaseDevice]):
        self.connected = False
        self._last_message_time: datetime = _EPOCH
        self._last_connect_time: datetime | None = None
        self.__mqtt_info = mqtt_info
        self.__devices: dict[str, BaseDevice] = devices

        from homeassistant.components.mqtt.async_client import AsyncMQTTClient

        self.__client: AsyncMQTTClient = AsyncMQTTClient(
            client_id=self.__mqtt_info.client_id,
            reconnect_on_failure=True,
            clean_session=True,
            callback_api_version=CallbackAPIVersion.VERSION2,
        )

        # self.__client._connect_timeout = 15.0
        self.__client.setup()
        self.__client.username_pw_set(self.__mqtt_info.username, self.__mqtt_info.password)
        self.__client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED)
        self.__client.tls_insecure_set(False)
        self.__client.on_connect = self._on_connect
        self.__client.on_disconnect = self._on_disconnect
        self.__client.on_message = self._on_message
        self.__client.on_socket_close = self._on_socket_close

        _LOGGER.info(
            f"Connecting to MQTT Broker {self.__mqtt_info.url}:{self.__mqtt_info.port} with client id {self.__mqtt_info.client_id} and username {self.__mqtt_info.username}"
        )
        self.__client.connect(self.__mqtt_info.url, self.__mqtt_info.port, keepalive=15)
        self.__client.loop_start()

    def is_connected(self):
        return self.connected and self.__client.is_connected()

    @property
    def last_message_time(self) -> datetime:
        return self._last_message_time

    @property
    def last_connect_time(self) -> datetime | None:
        return self._last_connect_time

    @property
    def has_seen_message(self) -> bool:
        return self._last_message_time > _EPOCH

    @callback
    def _on_socket_close(self, client: Client, userdata: Any, sock: Any) -> None:
        self.connected = False
        _LOGGER.info(f"MQTT Socket disconnection : {str(sock)}")

    @callback
    def _on_connect(
        self, client: Client, userdata: Any, flags: ConnectFlags, rc: ReasonCode, properties: Properties | None = None
    ):
        if rc == 0:
            self.connected = True
            self._last_connect_time = dt.utcnow()
            target_topics = [(topic, 1) for topic in self.__target_topics()]
            self.__client.subscribe(target_topics)
            _LOGGER.info(f"Subscribed to MQTT topics {target_topics}")
        else:
            self.__log_with_reason("connect", client, userdata, rc)

    @callback
    def _on_disconnect(
        self,
        client: Client,
        userdata: Any,
        disconnect_flags: DisconnectFlags,
        reason_code: ReasonCode,
        properties: Properties | None,
    ) -> None:
        if not self.connected:
            # from homeassistant/components/mqtt/client.py
            # This function is re-entrant and may be called multiple times
            # when there is a broken pipe error.
            return
        self.connected = False
        if reason_code.is_failure:
            self.__log_with_reason("disconnect", client, userdata, reason_code)

    @callback
    def _on_message(self, client, userdata, message: MQTTMessage):
        try:
            self._last_message_time = dt.utcnow()
            for sn, device in self.__devices.items():
                if device.update_data(message.payload, message.topic):
                    _LOGGER.debug(f"Message for {sn} and Topic {message.topic} : {message.payload.hex()}")
        except UnicodeDecodeError as error:
            _LOGGER.error(f"UnicodeDecodeError: {error}. Ignoring message and waiting for the next one.")
        except Exception:
            _LOGGER.error("Unexpected error processing MQTT message on topic %s", message.topic, exc_info=True)

    def stop(self):
        self.__client.unsubscribe(self.__target_topics())
        self.__client.loop_stop()
        self.__client.disconnect()

    def __log_with_reason(self, action: str, client, userdata, reason_code: ReasonCode):
        _LOGGER.error(f"MQTT {action}: {reason_code.getName()} ({self.__mqtt_info.client_id}) - {userdata}")

    def publish(self, topic: str, message: PayloadType) -> None:
        try:
            info = self.__client.publish(topic, message, 1)
            _LOGGER.debug("Sending " + str(message) + " :" + str(info) + "(" + str(info.is_published()) + ")")
        except RuntimeError as error:
            _LOGGER.error("Error on topic %s and message %s: %s", topic, message, error)
        except Exception as error:
            _LOGGER.debug("Error on topic %s and message %s: %s", topic, message, error)

    def __target_topics(self) -> list[str]:
        topics = []
        for device in self.__devices.values():
            for topic in device.device_info.topics():
                topics.append(topic)
        # Remove duplicates that can occur when multiple devices have the same topic (for example sub devices)
        return list(set(topics))
