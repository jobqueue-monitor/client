import asyncio
import signal
from dataclasses import dataclass
from typing import Any

import asyncssh
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Label

from .query import shutdown
from .screens import JobScreen, QueueDetailScreen, QueueScreen


async def cleanup(local_port):
    await shutdown(local_port)

    for task in asyncio.all_tasks():
        task.cancel()


class SSHConnected(Message):
    def __init__(self, connection):
        super().__init__()
        self.connection = connection


class ServerStarted(Message):
    pass


async def search_executable(con, executable):
    result = await con.run("bash -c 'echo $HOME'")
    home = result.stdout.strip()

    return f"{home}/.cargo/bin/{executable}"


@dataclass
class Config:
    server: str | None = None
    remote_port: int = 11203
    local_port: int = 11203

    server_executable: str | None = None


class JobqueueMonitor(App):
    TITLE = "jobqueue-monitor"

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit", show=True, priority=True),
        Binding("q", "show_queues", "Queues"),
        Binding("j", "show_jobs", "Jobs"),
    ]

    SCREENS = {
        "queue": QueueScreen,
        "queue_details": QueueDetailScreen,
        "job": JobScreen,
    }
    CSS_PATH = "jobqueue_monitor.tcss"
    loading = reactive(False, bindings=True)

    def __init__(self, config: Config):
        self.config = config
        self.SUB_TITLE = f"monitor the status of the jobqueue on '{config.server}'"

        super().__init__()

        self._connection = None

    def compose(self) -> ComposeResult:
        yield Header()

        with Vertical(classes="welcome", id="welcome"):
            yield Label(f"PBS server at: {self.config.server}", classes="welcome")

            yield DataTable(
                id="server_table",
                cursor_type="none",
                zebra_stripes=True,
                classes="welcome",
            )

        yield Footer()

    @work(exclusive=True, group="connect", description="connecting to ssh server")
    async def _connect(self) -> None:
        server_table = self.query_one(DataTable)
        server_table.add_rows(
            [
                ("alias", self.config.server),
                ("local port", self.config.local_port),
                ("remote port", self.config.remote_port),
            ]
        )

        def signal_handler():
            return asyncio.create_task(cleanup(self.config.local_port))

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)

        connection = await asyncssh.connect(self.config.server)

        self.post_message(SSHConnected(connection=connection))

    @on(SSHConnected)
    def launch_server(self, message: SSHConnected) -> None:
        self._connection = message.connection

        self._launch_server()

    @work(exclusive=True, group="launch-server", description="launch the server")
    async def _launch_server(self) -> None:
        server_executable = self.config.server_executable or await search_executable(
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
            self.post_message(ServerStarted())

            # run indefinitely
            await asyncio.gather(proc.wait(), listener.wait_closed())

    @on(ServerStarted)
    def enable_table(self) -> None:
        self.loading = False

    def action_show_queues(self) -> None:
        self.push_screen("queue")

    def action_show_jobs(self) -> None:
        self.push_screen("job")

    def watch_loading(self, old_state: bool, new_state: bool) -> None:
        welcome = self.query_one(Vertical)
        welcome.loading = new_state

    def check_action(self, action: str, parameters: tuple[Any, ...]) -> bool | None:
        if self.loading and action in {"show_jobs", "show_queues"}:
            return None

        return True

    def on_mount(self) -> None:
        server_table = self.query_one(DataTable)
        server_table.add_columns("name", "value")

        self.loading = True

        self._connect()

    async def action_quit(self) -> None:
        await cleanup(self.config.local_port)
        self._connection.close()
