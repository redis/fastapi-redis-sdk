"""Tests for cache coders."""

from datetime import datetime, timezone
from uuid import UUID

import fakeredis.aioredis
import pytest
from pydantic import BaseModel

import redis_fastapi.types as types
from redis_fastapi.cache_backend import CacheBackend
from redis_fastapi.types import Coder, JsonCoder


@pytest.mark.unit
class TestJsonCoder:
    def test_round_trip_dict(self) -> None:
        data = {"hello": "world", "n": 42}
        encoded = JsonCoder.encode(data)
        assert isinstance(encoded, str)
        decoded = JsonCoder.decode(encoded)
        assert decoded == data

    def test_round_trip_list(self) -> None:
        data = [1, 2, "three"]
        assert JsonCoder.decode(JsonCoder.encode(data)) == data

    def test_round_trip_string(self) -> None:
        data = "plain string"
        assert JsonCoder.decode(JsonCoder.encode(data)) == data

    def test_round_trip_number(self) -> None:
        assert JsonCoder.decode(JsonCoder.encode(3.14)) == 3.14

    def test_round_trip_none(self) -> None:
        assert JsonCoder.decode(JsonCoder.encode(None)) is None

    def test_non_serialisable_raises(self) -> None:
        """encode raises TypeError on non-JSON-serializable types."""
        import datetime

        dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        with pytest.raises(TypeError):
            JsonCoder.encode(dt)

    def test_satisfies_coder_protocol(self) -> None:
        assert isinstance(JsonCoder, type)
        # Runtime check via Protocol
        assert issubclass(JsonCoder, Coder)


@pytest.mark.unit
class TestCustomCoder:
    def test_custom_coder_protocol(self) -> None:
        class ReverseCoder:
            @classmethod
            def encode(cls, value):
                return str(value)[::-1]

            @classmethod
            def decode(cls, value):
                return value[::-1]

        assert isinstance(ReverseCoder(), Coder)
        assert ReverseCoder.decode(ReverseCoder.encode("hello")) == "hello"


class Product(BaseModel):
    id: UUID
    name: str
    created_at: datetime


@pytest.mark.unit
class TestFastAPIJsonCoder:
    def test_encodes_fastapi_json_compatible_values(self) -> None:
        data = {
            "id": UUID("12345678-1234-5678-1234-567812345678"),
            "created_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        }

        decoded = types.FastAPIJsonCoder.decode(types.FastAPIJsonCoder.encode(data))

        assert decoded == {
            "id": "12345678-1234-5678-1234-567812345678",
            "created_at": "2026-01-02T03:04:05+00:00",
        }

    def test_encodes_pydantic_model_to_json_compatible_dict(self) -> None:
        product = Product(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Widget",
            created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        )

        decoded = types.FastAPIJsonCoder.decode(types.FastAPIJsonCoder.encode(product))

        assert decoded == {
            "id": "12345678-1234-5678-1234-567812345678",
            "name": "Widget",
            "created_at": "2026-01-02T03:04:05Z",
        }

    def test_satisfies_coder_protocol(self) -> None:
        assert issubclass(types.FastAPIJsonCoder, Coder)


@pytest.mark.unit
class TestPydanticModelCoder:
    def test_decodes_to_pydantic_model(self) -> None:
        product = Product(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Widget",
            created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        )
        coder = types.pydantic_model_coder(Product)

        decoded = coder.decode(coder.encode(product))

        assert decoded == product

    def test_encodes_json_compatible_dict_and_decodes_model(self) -> None:
        coder = types.pydantic_model_coder(Product)

        decoded = coder.decode(
            coder.encode(
                {
                    "id": "12345678-1234-5678-1234-567812345678",
                    "name": "Widget",
                    "created_at": "2026-01-02T03:04:05Z",
                }
            )
        )

        assert decoded == Product(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Widget",
            created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        )

    def test_satisfies_coder_protocol(self) -> None:
        assert issubclass(types.pydantic_model_coder(Product), Coder)


@pytest.mark.unit
class TestCoderExports:
    def test_fastapi_coders_exported_from_package_root(self) -> None:
        from redis_fastapi import FastAPIJsonCoder, pydantic_model_coder

        assert FastAPIJsonCoder is types.FastAPIJsonCoder
        assert pydantic_model_coder is types.pydantic_model_coder


@pytest.mark.unit
class TestCoderWithCacheBackend:
    @pytest.mark.asyncio
    async def test_pydantic_model_coder_round_trips_through_cache_backend(
        self,
    ) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        cache = CacheBackend(fake, coder=types.pydantic_model_coder(Product))
        product = Product(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            name="Widget",
            created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        )

        await cache.set("product:1", product, eviction_group="products")

        assert await cache.get("product:1", eviction_group="products") == product
