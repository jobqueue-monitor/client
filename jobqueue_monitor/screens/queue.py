import json
import pathlib

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Footer, Header, Static

from jobqueue_monitor.utils import natural_sort_key, translate_json

PATH = pathlib.Path(__file__).parent / "../../../dummy-server/qstat_queue.json"


def query_data(path):
    return translate_json(json.loads(path.read_text())["Queue"])


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

        rows = sorted(
            [extract_row(id, attrs) for id, attrs in self.data.items()],
            key=lambda x: natural_sort_key(x[0]),
        )

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


def render_permissions_table(data, **kwargs):
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
    return table


def render_settings_table(data, **kwargs):
    queue_info_keys = [
        "queue_type",
        "enabled",
        "started",
        "priority",
    ]
    table = DataTable(name="Settings", **kwargs)
    table.add_columns("name", "value")
    table.add_rows([(k, data.get(k, "(unset)")) for k in queue_info_keys])
    return table


def preprocess_resource_table(data):
    translations = {
        "memory": "mem",
        "# MPI processes": "mpiprocs",
        "# cpus": "ncpus",
        "# nodes": "nodect",
        "select": "select",
        "walltime": "walltime",
        "place": "place",
    }

    return {
        key: data.get(data_key, "(unset)") for key, data_key in translations.items()
    }


def render_resource_table(data, **kwargs):
    if data:
        translated = preprocess_resource_table(data)
        rows = list(translated.items())
        disabled = False
    else:
        rows = []
        disabled = True

    table = DataTable(disabled=disabled, **kwargs)
    table.add_columns("resource", "value")
    table.add_rows(rows)

    return table


def render_resource_limits(data, **kwargs):
    def preprocess_group_key(key):
        return key.removeprefix("resources").lstrip("_")

    expected_keys = [
        "mem",
        "ncpus",
        "nodect",
        "walltime",
        "mpiprocs",
        "place",
        "select",
    ]
    resource_limits = {
        key: {
            preprocess_group_key(group): data.get(group, {}).get(key, "(unset)")
            for group in ["resources_min", "resources_max", "resources_default"]
        }
        for key in expected_keys
    }
    translated = preprocess_resource_table(resource_limits)

    if any(
        value != "(unset)" for group in translated.values() for value in group.values()
    ):
        rows = [
            (resource,) + tuple(group.values())
            for resource, group in translated.items()
        ]
        disabled = False
    else:
        rows = []
        disabled = True

    table = DataTable(disabled=disabled, **kwargs)
    table.add_columns("resource", "min", "max", "default")
    table.add_rows(rows)

    return table


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

        # queue info
        with Vertical(id="queue_info", classes="queue_detail_container"):
            yield Static(
                "[i]Queue Info[/i]", id="queue_info_heading", classes="heading"
            )

            with Horizontal(id="queue_info_container"):
                with Vertical(id="queue_settings", classes="queue_details"):
                    yield Static(
                        "[i]Settings[/i]",
                        id="queue_settings_heading",
                        classes="heading",
                    )

                    yield render_settings_table(
                        self._data,
                        zebra_stripes=True,
                        id="queue_settings_table",
                        cursor_type="none",
                    )
                with Vertical(id="queue_permissions", classes="queue_details"):
                    yield Static(
                        "[i]Permissions[/i]",
                        id="queue_permissions_heading",
                        classes="heading",
                    )

                    yield render_permissions_table(
                        self._data,
                        zebra_stripes=True,
                        id="queue_permissions_table",
                        cursor_type="none",
                    )

        with Vertical(id="resource_info", classes="queue_detail_container"):
            yield Static(
                "[i]Resources[/i]", id="queue_resource_heading", classes="heading"
            )

            with Horizontal(id="resources"):
                with Vertical(id="assigned_resources", classes="queue_details"):
                    yield Static(
                        "[i]Assigned resources[/i]",
                        id="queue_resource_available_heading",
                        classes="heading",
                    )

                    yield render_resource_table(
                        self._data.get("resources_assigned", {}),
                        name="assigned_resources",
                        zebra_stripes=True,
                        id="queue_resource_assigned_table",
                        cursor_type="none",
                    )

                with Vertical(id="resource_limits", classes="queue_details"):
                    yield Static(
                        "[i]Resource limits[/i]",
                        id="queue_resource_limits_heading",
                        classes="heading",
                    )

                    yield render_resource_limits(
                        self._data,
                        name="resource_limits",
                        zebra_stripes=True,
                        id="queue_resource_assigned_table",
                        cursor_type="none",
                    )

        yield render_job_summary(self._data)
        yield Footer()
