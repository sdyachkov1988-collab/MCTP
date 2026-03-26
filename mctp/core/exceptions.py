class MCTPError(Exception):
    pass


class ConfigVersionError(MCTPError):
    pass


class DecimalRequiredError(MCTPError):
    pass


class UTCRequiredError(MCTPError):
    pass


class RiskLimitError(MCTPError):
    pass


class PositionProtectionError(MCTPError):
    pass
