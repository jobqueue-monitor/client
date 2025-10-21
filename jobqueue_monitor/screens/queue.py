import re

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.message import Message
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Footer, Header, Input, Static

from jobqueue_monitor.query import query
from jobqueue_monitor.utils import natural_sort_key


class QueueQueryResult(Message):
    def __init__(self, data):
        super().__init__()

        self.data = data


def extract_row(id, data):
    attrs = data["attributes"]
    description = data["description"] or "(missing)"

    return (id, attrs["queue_type"], attrs["total_jobs"], description)


def update_queue_table(table, data):
    rows = sorted(
        [extract_row(id, attrs) for id, attrs in data.items()],
        key=lambda x: natural_sort_key(x[0]),
    )

    table.clear(columns=False)
    table.add_rows(rows)


class QueueScreen(Screen):
    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("ctrl+g", "refresh", "Refresh"),
        ("ctrl+k", "search", "Search queues"),
    ]
    CSS_PATH = "queue.tcss"

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="queues"):
            yield Static("[i]Queues[/i]", id="queues_heading", classes="heading")
            yield Input(
                placeholder="Search queue",
                type="text",
                id="queue_search_bar",
                select_on_focus=True,
            )
            with Container():
                yield DataTable(
                    classes="queues_table", cursor_type="row", zebra_stripes=True
                )
        yield Footer()

    def action_refresh(self):
        self.refresh_queue_table()

    @work(exclusive=True, group="query-queue", description="query the queues")
    async def refresh_queue_table(self) -> None:
        from textual.app import App

        app = self.query_ancestor(App)
        data = await query(app.config.local_port, kind="queue")

        self.post_message(QueueQueryResult(data=data))

    @on(QueueQueryResult)
    def refresh_data(self, message: QueueQueryResult):
        self.data = message.data

        table = self.query_one(DataTable)
        update_queue_table(table, self.data)
        table.loading = False
        table.focus()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns("name", "type", "# jobs", "description")
        table.loading = True
        table.focus()

        self.refresh_queue_table()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table = self.query_one(DataTable)

        row = table.get_row_at(event.cursor_row)
        id = row[0]

        self.app.push_screen(QueueDetailScreen(id=id, data=self.data[id]))

    def on_input_changed(self, event: Input.Changed) -> None:
        input_widget = self.query_one(Input)
        table = self.query_one(DataTable)

        expression_re = re.compile(input_widget.value)

        new_data = {
            key: value
            for key, value in self.data.items()
            if any(expression_re.match(v) for v in extract_row(key, value))
        }
        update_queue_table(table, new_data)

    def on_input_submitted(self):
        table = self.query_one(DataTable)
        table.focus()

    def action_search(self):
        input_widget = self.query_one(Input)
        input_widget.focus()


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
    group_names = [
        "resources_assigned",
        "resources_min",
        "resources_max",
        "resources_default",
    ]
    resource_limits = {
        key: {
            preprocess_group_key(group): data.get(group, {}).get(key, "(unset)")
            for group in group_names
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
    table.add_columns("resource", *[k.removeprefix("resources_") for k in group_names])
    table.add_rows(rows)

    return table


def parse_state_count(string):
    parts = (part.split(":") for part in string.strip().split())
    return {name.lower(): int(count) for name, count in parts}


def render_job_summary(data, **kwargs):
    total = data["total_jobs"]
    state_count = parse_state_count(data["state_count"])

    table = DataTable(**kwargs)
    table.add_columns("kind", "count")
    rows = list(state_count.items()) + [("total", total)]
    table.add_rows(rows)

    return table


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
                        self._data["attributes"],
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

            yield render_resource_table(
                self._data["attributes"],
                name="resources",
                zebra_stripes=True,
                id="queue_resource_table",
                cursor_type="none",
            )

        with Vertical(classes="queue_detail_container", id="queue_job_summary"):
            yield Static(
                "[i]Job Summary[/i]", id="queue_job_summary_heading", classes="heading"
            )

            yield render_job_summary(
                self._data["attributes"],
                name="job_summary",
                id="queue_job_summary_table",
                cursor_type="none",
            )

        yield Footer()
