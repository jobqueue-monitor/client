from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Footer, Header, Static

DUMMY_TEXT = """\
Dummy text in replacement for the queue table.
"""


class QueueScreen(Screen):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(DUMMY_TEXT)
        yield Footer()
