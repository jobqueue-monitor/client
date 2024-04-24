from textual.app import App, ComposeResult
from textual.widgets import Header


class JobqueueMonitor(App):
    TITLE = "jobqueue-monitor"

    def __init__(self, server: str | None = None):
        self.server = server
        self.SUB_TITLE = f"monitor the status of the jobqueue on '{server}'"

        super().__init__()

    def compose(self) -> ComposeResult:
        yield Header()
