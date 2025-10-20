import asyncio
import signal
from dataclasses import dataclass

import asyncssh
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.message import Message
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
        Binding("q", "push_screen('queue')", "Queue"),
        Binding("j", "push_screen('job')", "Job"),
    ]

    SCREENS = {
        "queue": QueueScreen,
        "queue_details": QueueDetailScreen,
        "job": JobScreen,
    }
    CSS_PATH = "jobqueue_monitor.tcss"

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
    def enable_table(self):
        welcome = self.query_one(Vertical)
        welcome.loading = False

    def on_mount(self):
        server_table = self.query_one(DataTable)
        server_table.add_columns("name", "value")

        welcome = self.query_one(Vertical)
        welcome.loading = True

        self._connect()

    async def action_quit(self):
        await cleanup(self.config.local_port)
        self._connection.close()
