from mctp.core.exceptions import MCTPError


class StorageError(MCTPError):
    pass


class StorageCorruptedError(StorageError):
    pass


class StorageSchemaMismatchError(StorageError):
    pass
