import enum


class CraveResultsLogType(enum.IntEnum):
    INIT = enum.auto()
    HYPERPARAMS = enum.auto()
    LOG = enum.auto()
    LOG_SUMMARY = enum.auto()
    LOG_HISTORY = enum.auto()
    LOG_ARTIFACT = enum.auto()
    BINARY = enum.auto()
    REMOVE_EXPERIMENT = enum.auto()
