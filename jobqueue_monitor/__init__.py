from importlib.metadata import version

try:
    __version__ = version("jobqueue_monitor")
except Exception:
    __version__ = "9999"
