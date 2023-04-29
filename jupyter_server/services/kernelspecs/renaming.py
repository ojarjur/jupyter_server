"""Support for renaming kernel specs at runtime."""
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from functools import wraps
from typing import Any

from jupyter_client.kernelspec import KernelSpecManager
from jupyter_core.utils import ensure_async, run_sync
from traitlets import HasTraits, Unicode, default


def normalize_kernel_name(method):
    @wraps(method)
    async def wrapped_method(self, *args, **kwargs):
        kernel_name = kwargs.get("kernel_name", None)
        if (
            kernel_name
            and hasattr(self, "kernel_spec_manager")
            and hasattr(self.kernel_spec_manager, "original_kernel_name")
        ):
            kwargs["kernel_name"] = self.kernel_spec_manager.original_kernel_name(kernel_name)
        return await method(self, *args, **kwargs)

    return wrapped_method


class RenamingKernelSpecManagerMixin(HasTraits):
    """KernelSpecManager mixin that renames kernel specs.

    The base KernelSpecManager class only has synchronous methods, but some child
    classes (in particular, GatewayKernelManager) change those methods to be async.

    In order to support both versions, we provide both synchronous and async versions
    of all the relevant kernel spec manager methods. We first do the renaming in the
    async version, but override the KernelSpecManager base methods using the
    synchronous versions.
    """

    spec_name_prefix = Unicode(
        config=True, help="Prefix to be added onto the front of kernel spec names."
    )

    spec_name_format = Unicode(
        config=True,
        help="""Format for rewritten kernel spec names.

        Defaults to prefixing the kernel spec name with the value of the
        `spec_name_prefix` attribute if it has been set.
        """,
    )

    @default("spec_name_format")
    def _default_spec_name_format(self):
        if self.spec_name_prefix:
            return self.spec_name_prefix + "{}"
        return "{}"

    display_name_suffix = Unicode(
        config=True, help="Suffix to be added onto the end of kernel spec display names."
    )

    display_name_format = Unicode(
        config=True, help="Format for rewritten kernel spec display names."
    )

    @default("display_name_format")
    def _default_display_name_format(self):
        if self.display_name_suffix:
            return "{}" + self.display_name_suffix
        return "{}"

    default_kernel_name = Unicode(allow_none=True)

    def rename_kernel(self, kernel_name: str) -> str:
        """Rename the supplied kernel spec based on the configured format string."""
        if not hasattr(self, "original_kernel_names"):
            self.original_kernel_names = {}

        renamed = self.spec_name_format.format(kernel_name)
        self.original_kernel_names[renamed] = kernel_name
        return renamed

    def maybe_rename_kernel(self, kernel_name: str) -> str:
        """Rename the supplied kernel if it is not already the result of a rename."""
        if not hasattr(self, "original_kernel_names"):
            self.original_kernel_names = {}
        if kernel_name in self.original_kernel_names:
            # The kernel was already renamed
            return kernel_name
        return self.rename_kernel(kernel_name)

    def original_kernel_name(self, kernel_name: str) -> str:
        if not hasattr(self, "original_kernel_names"):
            return kernel_name

        return self.original_kernel_names.get(kernel_name, kernel_name)

    async def async_get_all_specs(self):
        ks = {}
        original_ks = await ensure_async(super().get_all_specs())
        for s, k in original_ks.items():
            spec_name = s
            kernel_spec = k
            original_prefix = f"/kernelspecs/{spec_name}"
            spec_name = self.rename_kernel(spec_name)
            new_prefix = f"/kernelspecs/{spec_name}"

            ks[spec_name] = kernel_spec
            kernel_spec["name"] = spec_name
            kernel_spec["spec"] = kernel_spec.get("spec", {})
            kernel_spec["resources"] = kernel_spec.get("resources", {})

            spec = kernel_spec["spec"]
            spec["display_name"] = self.display_name_format.format(spec.get("display_name"))

            resources = kernel_spec["resources"]
            for name, value in resources.items():
                resources[name] = value.replace(original_prefix, new_prefix)
        if hasattr(super(), "default_kernel_name"):
            self.default_kernel_name = self.rename_kernel(super().default_kernel_name)
        return ks

    def get_all_specs(self):
        return run_sync(self.async_get_all_specs)()

    async def async_get_kernel_spec(self, kernel_name: str, *args: Any, **kwargs: Any):
        kernel_name = self.original_kernel_name(kernel_name)
        return await ensure_async(super().get_kernel_spec(kernel_name, *args, **kwargs))

    def get_kernel_spec(self, kernel_name: str, *args: Any, **kwargs: Any):
        return run_sync(self.async_get_kernel_spec)(kernel_name, *args, **kwargs)

    async def get_kernel_spec_resource(self, kernel_name: str, *args: Any, **kwargs: Any):
        if not hasattr(super(), "get_kernel_spec_resource"):
            return None
        kernel_name = self.original_kernel_name(kernel_name)
        return await ensure_async(super().get_kernel_spec_resource(kernel_name, *args, **kwargs))


class RenamingKernelSpecManager(RenamingKernelSpecManagerMixin, KernelSpecManager):
    """KernelSpecManager that renames kernels"""
