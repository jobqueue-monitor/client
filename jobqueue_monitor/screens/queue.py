import json
import pathlib

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Footer, Header, Static

PATH = pathlib.Path(__file__).parent / "../../../dummy-server/qstat_queue.json"


def query_data(path):
    return translate_json(json.loads(path.read_text())["Queue"])


def translate_json(data):
    match data:
        case "True" | "False" as obj:
            return bool(obj)
        case dict() as obj:
            return {k: translate_json(v) for k, v in obj.items()}
        case list() as obj:
            return [translate_json(v) for v in obj]
        case _ as obj:
            return obj


def extract_row(id, data):
    # attrs = data["attributes"]
    # description = data["description"] or "(missing)"

    attrs = data
    description = "(missing)"

    return (id, attrs["queue_type"], attrs["total_jobs"], description)


class QueueScreen(Screen):
    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
    ]
    CSS_PATH = "queue.tcss"

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(classes="queues_table")
        yield Footer()

    def refresh_data(self, table):
        self.data = query_data(PATH)

        rows = [extract_row(id, attrs) for id, attrs in self.data.items()]

        table.clear(columns=False)
        table.add_rows(rows)

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns("name", "type", "# jobs", "description")

        self.refresh_data(table)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table = self.query_one(DataTable)

        row = table.get_row_at(event.cursor_row)
        id = row[0]

        self.app.push_screen(QueueDetailScreen(id=id, data=self.data[id]))


class QueueInfo(Vertical):
    def __init__(self, data):
        label = Static("[i]Queue Info[/i]", id="queue_info_heading", classes="heading")
        settings = GeneralQueueInfo(data)
        permissions = PermissionsInfo(data)

        super().__init__(
            label,
            Horizontal(settings, permissions),
            id="queue_info",
            classes="queue_detail_container",
        )


class PermissionsInfo(Vertical):
    def __init__(self, data):
        def ensure_list(value):
            if value is None:
                return []
            elif isinstance(value, list):
                return value
            else:
                return [value]

        # keys:
        # - "acl_user_enable"
        # - "acl_users"
        users = ensure_list(data.get("acl_users"))
        enforced = bool(data.get("acl_user_enable", False))

        label = Static(
            "[i]Permissions[/i]", id="queue_permissions_heading", classes="heading"
        )
        table = DataTable(
            name="Permissions",
            zebra_stripes=True,
            id="queue_permissions_table",
            cursor_type="none",
        )
        table.add_columns("name", "value")
        rows = [
            ("enforced", enforced),
            ("users", ", ".join(users)),
        ]
        table.add_rows(rows)

        super().__init__(
            label,
            table,
            name="Permissions",
            id="queue_permissions",
            classes="queue_details",
        )


class GeneralQueueInfo(Vertical):
    def __init__(self, data):
        queue_info_keys = [
            "queue_type",
            "enabled",
            "started",
            "priority",
        ]

        label = Static(
            "[i]Settings[/i]", id="queue_settings_heading", classes="heading"
        )
        table = DataTable(
            name="Settings",
            zebra_stripes=True,
            id="queue_settings_table",
            cursor_type="none",
        )
        table.add_columns("name", "value")
        table.add_rows([(k, data.get(k, "(unset)")) for k in queue_info_keys])

        super().__init__(
            label,
            table,
            name="Settings",
            id="queue_general_info",
            classes="queue_details",
        )


def render_resource_info(data):
    return Static("resource info", classes="queue_detail_container")


def parse_state_count(string):
    parts = (part.split(":") for part in string.strip().split())
    return {name.lower(): int(count) for name, count in parts}


def render_job_summary(data):
    total = data["total_jobs"]
    state_count = parse_state_count(data["state_count"])

    label = Static(
        "[i]Job Summary[/i]",
        id="queue_job_summary_heading",
        classes="heading",
    )
    table = DataTable(
        name="Job Summary",
        id="queue_job_summary_table",
        cursor_type="none",
    )
    table.add_columns("kind", "count")
    rows = list(state_count.items()) + [("total", total)]
    table.add_rows(rows)

    return Vertical(
        label,
        table,
        classes="queue_detail_container",
        id="queue_job_summary",
    )


class QueueDetailScreen(ModalScreen):
    """Screen for displaying the details of a queue."""

    CSS_PATH = "queue_details.tcss"

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
    ]

    def __init__(self, id, data):
        self._queue_id = id
        self._data = data

        super().__init__()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(
            f"[b]Queue: {self._queue_id}[/b]", id="queue_name", classes="queue_details"
        )
        yield QueueInfo(self._data)
        yield render_resource_info(self._data)
        yield render_job_summary(self._data)
        yield Footer()
