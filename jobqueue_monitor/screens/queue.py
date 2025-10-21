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
        data = await query(self.app.config.local_port, kind="queue")

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


def update_permissions_table(table, data):
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

    rows = [
        ("enforced", enforced),
        ("users", ", ".join(users)),
    ]
    table.clear(columns=False)
    table.add_rows(rows)


def update_settings_table(table, data):
    queue_info_keys = [
        "queue_type",
        "enabled",
        "started",
        "priority",
    ]

    table.clear(columns=False)
    table.add_rows([(k, data.get(k, "(unset)")) for k in queue_info_keys])


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


def update_resource_table(table, data):
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

    table.clear(columns=False)
    table.add_rows(rows)
    table.disabled = disabled


def parse_state_count(string):
    parts = (part.split(":") for part in string.strip().split())
    return {name.lower(): int(count) for name, count in parts}


def update_job_summary(table, data):
    total = data["total_jobs"]
    state_count = parse_state_count(data["state_count"])

    rows = list(state_count.items()) + [("total", total)]
    table.clear(columns=False)
    table.add_rows(rows)


class QueueDetailScreen(ModalScreen):
    """Screen for displaying the details of a queue."""

    CSS_PATH = "queue_details.tcss"

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("ctrl+g", "refresh", "Refresh"),
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

                    yield DataTable(
                        id="settings", zebra_stripes=True, cursor_type="none"
                    )
                with Vertical(id="queue_permissions", classes="queue_details"):
                    yield Static(
                        "[i]Permissions[/i]",
                        id="queue_permissions_heading",
                        classes="heading",
                    )

                    yield DataTable(
                        name="Permissions",
                        id="permissions",
                        zebra_stripes=True,
                        cursor_type="none",
                    )

        with Vertical(id="resource_info", classes="queue_detail_container"):
            yield Static(
                "[i]Resources[/i]", id="queue_resource_heading", classes="heading"
            )

            yield DataTable(id="resources", zebra_stripes=True, cursor_type="none")

        with Vertical(classes="queue_detail_container", id="queue_job_summary"):
            yield Static(
                "[i]Job Summary[/i]", id="queue_job_summary_heading", classes="heading"
            )

            yield DataTable(id="job_summary", zebra_stripes=True, cursor_type="none")

        yield Footer()

    def on_mount(self) -> None:
        settings = self.query_one("DataTable#settings")
        settings.add_columns("name", "value")
        update_settings_table(settings, self._data["attributes"])

        permissions = self.query_one("DataTable#permissions")
        permissions.add_columns("name", "value")
        update_permissions_table(permissions, self._data["attributes"])

        resources = self.query_one("DataTable#resources")
        resources.add_columns("resource", "assigned", "min", "max", "default")
        update_resource_table(resources, self._data["attributes"])

        job_summary = self.query_one("DataTable#job_summary")
        job_summary.add_columns("kind", "count")
        update_job_summary(job_summary, self._data["attributes"])

    def action_refresh(self) -> None:
        self.refresh_content()

    @work(exclusive=True)
    async def refresh_content(self) -> None:
        data = await query(self.app.config.local_port, kind="queue")

        self.post_message(QueueQueryResult(data=data))

        self.data = data[self._queue_id]
        self.refresh_data()

    def refresh_data(self) -> None:
        settings = self.query_one("DataTable#settings")
        update_settings_table(settings, self._data["attributes"])

        permissions = self.query_one("DataTable#permissions")
        update_permissions_table(permissions, self._data["attributes"])

        resources = self.query_one("DataTable#resources")
        update_resource_table(resources, self._data["attributes"])

        job_summary = self.query_one("DataTable#job_summary")
        update_job_summary(job_summary, self._data["attributes"])
