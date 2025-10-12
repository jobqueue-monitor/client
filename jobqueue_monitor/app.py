from dataclasses import dataclass

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Header

from .screens import JobScreen, QueueDetailScreen, QueueScreen


@dataclass
class Config:
    server: str | None = None


class JobqueueMonitor(App):
    TITLE = "jobqueue-monitor"

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit", show=True, priority=True),
        Binding("q", "push_screen('queue')", "Queue"),
        Binding("j", "push_screen('job')", "Job"),
    ]

    SCREENS = {
        "queue": QueueScreen,
        "queue_details": QueueDetailScreen,
        "job": JobScreen,
    }

    def __init__(self, config: Config):
        self.config = config
        self.SUB_TITLE = f"monitor the status of the jobqueue on '{config.server}'"

        super().__init__()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()
