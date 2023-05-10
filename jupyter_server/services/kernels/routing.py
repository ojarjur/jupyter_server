import typing as t

from jupyter_client.kernelspec import KernelSpecManager
from jupyter_client.manager import in_pending_state
from jupyter_client.managerabc import KernelManagerABC
from jupyter_core.utils import ensure_async, run_sync
from traitlets import (
    Dict,
    Instance,
    List,
    Unicode,
)

from .connection.base import BaseKernelWebsocketConnection
from .connection.channels import ZMQChannelsWebsocketConnection
from .kernelmanager import AsyncMappingKernelManager, ServerKernelManager


class AsyncRoutingKernelSpecManager(KernelSpecManager):
    """KernelSpecManager that routes to multiple nested kernel spec managers.

    This async version of the wrapper exists because the base KernelSpecManager
    class only has synchronous methods, but some child classes (in particular,
    GatewayKernelManager) change those methods to be async.

    In order to support both versions, we first implement the routing in this async
    class, but then make it synchronous in the child, RoutingKernelSpecManager class.
    """

    default_manager = Instance(AsyncMappingKernelManager)

    additional_managers = List(trait=Instance(AsyncMappingKernelManager))

    spec_to_manager_map = Dict(key_trait=Unicode(), value_trait=Instance(AsyncMappingKernelManager))

    async def get_all_specs(self):
        ks = await ensure_async(self.default_manager.kernel_spec_manager.get_all_specs())
        for spec_name, _spec in ks.items():
            self.spec_to_manager_map[spec_name] = self.default_manager
        for additional_manager in self.additional_managers:
            additional_ks = await ensure_async(
                additional_manager.kernel_spec_manager.get_all_specs()
            )
            for spec_name, spec in additional_ks.items():
                if spec_name not in ks:
                    ks[spec_name] = spec
                    self.spec_to_manager_map[spec_name] = additional_manager
        return ks

    def get_mapping_kernel_manager(self, kernel_name: str) -> AsyncMappingKernelManager:
        km = self.spec_to_manager_map.get(kernel_name, None)
        if km is None:
            return self.default_manager
        return km

    def get_mapping_kernel_manager_for_kernel(self, kernel_id: str) -> AsyncMappingKernelManager:
        if kernel_id in self.default_manager.list_kernels():
            return self.default_manager
        for manager in self.additional_managers:
            if kernel_id in manager.list_kernels():
                return manager
        return self.default_manager

    async def get_kernel_spec(self, kernel_name, **kwargs):
        wrapped_manager = self.get_mapping_kernel_manager(kernel_name).kernel_spec_manager
        return ensure_async(wrapped_manager.get_kernel_spec(kernel_name, **kwargs))

    async def get_kernel_spec_resource(self, kernel_name, path):
        wrapped_manager = self.get_mapping_kernel_manager(kernel_name)
        wrapped_kernelspec_manager = wrapped_manager.kernel_spec_manager
        self.log.debug(
            "Getting kernel spec resource for {} from {}/{}".format(
                kernel_name, wrapped_manager, wrapped_kernelspec_manager
            )
        )
        if hasattr(wrapped_kernelspec_manager, "get_kernel_spec_resource"):
            return await ensure_async(
                wrapped_kernelspec_manager.get_kernel_spec_resource(kernel_name, path)
            )
        return None


class RoutingKernelSpecManager(AsyncRoutingKernelSpecManager):
    """KernelSpecManager that routes to multiple nested kernel spec managers."""

    def get_all_specs(self):
        return run_sync(super().get_all_specs)()

    def get_kernel_spec(self, kernel_name, *args, **kwargs):
        return run_sync(super().get_kernel_spec)(kernel_name, *args, **kwargs)


class RoutingKernelManagerWebsocketConnection(BaseKernelWebsocketConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        km = self.kernel_manager.wrapped_kernel_manager
        wrapped_class = ZMQChannelsWebsocketConnection
        if hasattr(km, "websocket_connection_class"):
            wrapped_class = km.websocket_connection_class
        self.wrapped = wrapped_class(
            parent=km, websocket_handler=self.websocket_handler, config=self.config
        )

    async def connect(self):
        """Connect the kernel websocket to the kernel ZMQ connections"""
        return await self.wrapped.connect()

    async def disconnect(self):
        """Disconnect the kernel websocket from the kernel ZMQ connections"""
        return await self.wrapped.disconnect()

    def handle_incoming_message(self, incoming_msg: str) -> None:
        """Broker the incoming websocket message to the appropriate ZMQ channel."""
        self.wrapped.handle_incoming_message(incoming_msg)

    def handle_outgoing_message(self, stream: str, outgoing_msg: list) -> None:
        """Broker outgoing ZMQ messages to the kernel websocket."""
        self.wrapped.handle_outgoing_message(stream, outgoing_msg)

    async def prepare(self):
        if hasattr(self.wrapped, "prepare"):
            self.wrapped.prepare()
        self.session = self.wrapped.session
        self.session.key = self.wrapped.kernel_manager.session.key


class RoutingKernelManager(ServerKernelManager):
    kernel_id_map: t.Dict[str, str] = {}

    @property
    def wrapped_multi_kernel_manager(self):
        return self.parent.kernel_spec_manager.get_mapping_kernel_manager(self.kernel_name)

    @property
    def wrapped_kernel_manager(self):
        if not self.kernel_id:
            return None
        wrapped_kernel_id = RoutingKernelManager.kernel_id_map.get(self.kernel_id, self.kernel_id)
        return self.wrapped_multi_kernel_manager.get_kernel(wrapped_kernel_id)

    @property
    def websocket_connection_class(self):
        return RoutingKernelManagerWebsocketConnection

    @property
    def has_kernel(self):
        if not self.kernel_id:
            return False
        return self.wrapped_kernel_manager.has_kernel

    def client(self, *args, **kwargs):
        if not self.kernel_id:
            return None
        return self.wrapped_kernel_manager.client(*args, **kwargs)

    @in_pending_state
    async def start_kernel(self, *args, **kwargs):
        kernel_id: t.Optional[str] = kwargs.pop("kernel_id", self.kernel_id)
        if kernel_id:
            self.kernel_id = kernel_id

        km = self.wrapped_multi_kernel_manager
        wrapped_kernel_id: str = await ensure_async(
            km.start_kernel(kernel_name=self.kernel_name, **kwargs)
        )
        self.kernel_id = self.kernel_id or wrapped_kernel_id
        RoutingKernelManager.kernel_id_map[self.kernel_id] = wrapped_kernel_id
        self.log.debug(
            f"Created kernel {self.kernel_id} corresponding to {wrapped_kernel_id} in {km}"
        )
        self.log.debug(RoutingKernelManager.kernel_id_map)

    async def shutdown_kernel(self, now=False, restart=False):
        wrapped_kernel_id = RoutingKernelManager.kernel_id_map.get(self.kernel_id, self.kernel_id)
        km = self.wrapped_multi_kernel_manager
        await ensure_async(km.shutdown_kernel(wrapped_kernel_id, now=now, restart=restart))
        RoutingKernelManager.kernel_id_map.pop(self.kernel_id, None)

    async def restart_kernel(self, now=False):
        wrapped_kernel_id = RoutingKernelManager.kernel_id_map.get(self.kernel_id, self.kernel_id)
        km = self.wrapped_multi_kernel_manager
        return await ensure_async(km.restart_kernel(wrapped_kernel_id, now=now))

    async def interrupt_kernel(self):
        wrapped_kernel_id = RoutingKernelManager.kernel_id_map.get(self.kernel_id, self.kernel_id)
        km = self.wrapped_kernel_manager
        return await ensure_async(km.interrupt_kernel(wrapped_kernel_id))


KernelManagerABC.register(RoutingKernelManager)
