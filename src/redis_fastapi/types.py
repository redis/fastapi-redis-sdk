"""Shared types for fastapi-redis-sdk."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, TypeAlias, TypeVar, runtime_checkable

from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)


@runtime_checkable
class Coder(Protocol):
    """Protocol for encoding/decoding cached values."""

    @classmethod
    def encode(cls, value: Any) -> str: ...  # pragma: no cover

    @classmethod
    def decode(cls, value: str) -> Any: ...  # pragma: no cover


class JsonCoder:
    """Default JSON coder using stdlib json."""

    @classmethod
    def encode(cls, value: Any) -> str:
        return json.dumps(value)

    @classmethod
    def decode(cls, value: str) -> Any:
        return json.loads(value)


def pydantic_model_coder(model_type: type[ModelT]) -> type[Coder]:
    """Create a coder that decodes cached JSON back into *model_type*.

    The returned coder accepts either an instance of *model_type* or data that
    Pydantic can validate into that model.
    """

    class _PydanticModelCoder:
        @classmethod
        def encode(cls, value: Any) -> str:
            model: ModelT
            if isinstance(value, model_type):
                model = value
            else:
                model = model_type.model_validate(value)
            return model.model_dump_json()

        @classmethod
        def decode(cls, value: str) -> ModelT:
            return model_type.model_validate_json(value)

    _PydanticModelCoder.__name__ = f"{model_type.__name__}Coder"
    _PydanticModelCoder.__qualname__ = _PydanticModelCoder.__name__
    return _PydanticModelCoder


@runtime_checkable
class Encryptor(Protocol):
    """Protocol for encrypting a serialized payload at rest.

    Encryption **wraps** serialization, never the reverse: the ``Coder`` turns
    the value into bytes, and only then does the encryptor see them.  A coder
    must never be handed ciphertext.

    This package ships the seam and not an implementation, deliberately - see
    the recipe in the sessions guide.  Ten lines of ``AESGCM`` in your own
    codebase is a smaller liability for everyone than a cryptographic
    primitive maintained here.
    """

    def encrypt(self, data: bytes) -> bytes: ...  # pragma: no cover

    def decrypt(self, data: bytes) -> bytes: ...  # pragma: no cover


# A key builder receives (request, eviction_group, prefix) and returns a cache key.
KeyBuilder: TypeAlias = Callable[..., str | Awaitable[str]]
