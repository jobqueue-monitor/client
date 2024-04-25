from dataclasses import dataclass

from textual.app import App, ComposeResult
from textual.widgets import Footer, Header


@dataclass
class Config:
    server: str | None = None


class JobqueueMonitor(App):
    TITLE = "jobqueue-monitor"

    def __init__(self, config: Config):
        self.config = config
        self.SUB_TITLE = f"monitor the status of the jobqueue on '{config.server}'"

        super().__init__()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()
