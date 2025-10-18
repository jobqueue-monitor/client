import asyncio
from dataclasses import dataclass

import asyncssh
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.message import Message
from textual.widgets import Footer, Header

from .screens import QueueDetailScreen, QueueScreen


class SSHConnected(Message):
    def __init__(self, connection):
        super().__init__()
        self.connection = connection


async def search_executable(con, executable):
    command = f"bash -c 'which {executable}'"
    result = await con.run(command)

    return result


@dataclass
class Config:
    server: str | None = None
    remote_port: int = 11203
    local_port: int = 11203


class JobqueueMonitor(App):
    TITLE = "jobqueue-monitor"

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit", show=True, priority=True),
        Binding("q", "push_screen('queue')", "Queue"),
    ]

    SCREENS = {"queue": QueueScreen, "queue_details": QueueDetailScreen}

    def __init__(self, config: Config):
        self.config = config
        self.SUB_TITLE = f"monitor the status of the jobqueue on '{config.server}'"

        super().__init__()

        self._connection = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

    @work(exclusive=True, group="connect", description="connecting to ssh server")
    async def _connect(self) -> None:
        connection = await asyncssh.connect(self.config.server)

        self.post_message(SSHConnected(connection=connection))

    @on(SSHConnected)
    def launch_server(self, message: SSHConnected) -> None:
        self._connection = message.connection

        self.server_worker = self._launch_server()

    @work(exclusive=True, group="launch-server", description="launch the server")
    async def _launch_server(self) -> None:
        server_executable = search_executable(
            self._connection,
            "jobqueue-monitor-server",
        )
        command = f"{server_executable} -p {self.config.remote_port}"
        async with self._connection.create_process(command) as proc:
            listener = await self._connection.forward_local_port(
                "localhost",
                self.config.local_port,
                "localhost",
                self.config.remote_port,
            )
            # run indefinitely
            await asyncio.gather(proc.wait(), listener.wait_closed())

    def on_mount(self):
        self._connect()

    def on_quit(self):
        self._connection.close()
