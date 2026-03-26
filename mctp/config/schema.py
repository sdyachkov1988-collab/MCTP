from dataclasses import dataclass

from mctp.core.constants import CONFIG_SCHEMA_VERSION
from mctp.core.exceptions import ConfigVersionError
from mctp.storage.exceptions import StorageSchemaMismatchError


@dataclass
class ConfigSchema:
    schema_version: str

    def validate(self) -> None:
        if self.schema_version != CONFIG_SCHEMA_VERSION:
            raise ConfigVersionError(
                f"Config schema mismatch: expected {CONFIG_SCHEMA_VERSION}, got {self.schema_version}"
            )


def validate_schema_version(config_dict: dict) -> None:
    """
    Проверить schema_version в словаре конфига.
    Отсутствие ключа или несовпадение → StorageSchemaMismatchError.
    """
    if "schema_version" not in config_dict:
        raise StorageSchemaMismatchError("Config schema_version key missing")
    found = config_dict["schema_version"]
    if found != CONFIG_SCHEMA_VERSION:
        raise StorageSchemaMismatchError(
            f"Config schema version mismatch: expected {CONFIG_SCHEMA_VERSION}, got {found}"
        )
