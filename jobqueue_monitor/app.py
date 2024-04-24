from textual.app import App, ComposeResult
from textual.widgets import Header


class JobqueueMonitor(App):
    TITLE = "jobqueue-monitor"
    SUBTITLE = "monitor the status of a jobqueue"

    def compose(self) -> ComposeResult:
        yield Header()
