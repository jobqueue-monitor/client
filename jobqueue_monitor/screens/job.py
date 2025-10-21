import datetime as dt
import re

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.message import Message
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Footer, Header, Input, Label

from jobqueue_monitor.query import query
from jobqueue_monitor.utils import natural_sort_key


class JobQueryResult(Message):
    def __init__(self, data):
        super().__init__()

        self.data = data


job_states = {
    "R": "running",
    "Q": "queued",
    "H": "on hold",
    "B": "begun",
    "E": "exiting",
    "F": "finished",
    "M": "moved",
    "S": "suspended",
    "T": "transiting",
    "W": "waiting",
    "U": "user suspended",
    "X": "expired",
}


def extract_row(id, data):
    attrs = {k.lower(): v for k, v in data["attributes"].items()}

    job_state = attrs["job_state"]
    return (
        id,
        attrs["queue"],
        job_states.get(job_state, job_state),
        attrs["job_name"],
        attrs["job_owner"],
        attrs.get("resources_used", {}).get("walltime", "(not running)"),
    )


def update_job_table(table, data):
    rows = sorted(
        [extract_row(id, attrs) for id, attrs in data.items()],
        key=lambda x: natural_sort_key(x[0]),
    )
    table.clear(columns=False)
    table.add_rows(rows)


class JobScreen(Screen):
    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("ctrl+g", "refresh", "Refresh"),
        ("ctrl+k", "search", "Search jobs"),
    ]

    CSS_PATH = "job.tcss"

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="jobs"):
            yield Label("[i]Jobs[/i]", id="jobs_heading", classes="heading")
            yield Input(
                placeholder="Search jobs",
                type="text",
                id="job_search_bar",
                select_on_focus=True,
            )

            with Container():
                yield DataTable(
                    classes="jobs_table",
                    cursor_type="row",
                    zebra_stripes=True,
                    id="jobs",
                )
        yield Footer()

    def action_refresh(self):
        self.refresh_job_table()

    @work(exclusive=True, group="query-job")
    async def refresh_job_table(self) -> None:
        data = await query(self.app.config.local_port, kind="job")

        self.post_message(JobQueryResult(data=data))

    @on(JobQueryResult)
    def refresh_data(self, message: JobQueryResult) -> None:
        self.data = message.data

        table = self.query_one("DataTable#jobs")

        update_job_table(table, self.data)
        table.loading = False
        table.focus()

    def on_mount(self) -> None:
        self.data = {}

        table = self.query_one(DataTable)
        table.add_columns("id", "queue", "status", "name", "owner", "walltime")

        self.refresh_job_table()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table = self.query_one(DataTable)

        row = table.get_row_at(event.cursor_row)
        id = row[0]

        self.app.push_screen(JobDetailScreen(id=id, data=self.data[id]))

    def on_input_changed(self, event: Input.Changed) -> None:
        search_bar = self.query_one(Input)
        table = self.query_one(DataTable)

        expression_re = re.compile(search_bar.value)

        new_data = {
            key: value
            for key, value in self.data.items()
            if any(expression_re.match(v) is not None for v in extract_row(key, value))
        }
        update_job_table(table, new_data)

    def on_input_submitted(self):
        table = self.query_one(DataTable)

        table.focus()

    def action_search(self):
        search_bar = self.query_one(Input)

        search_bar.focus()


job_translations = {
    "ctime": "creation time",
    "qtime": "queue time",
    "stime": "start time",
    "mtime": "last modification",
}


def update_job_details(table, data):
    keys = [
        "job_name",
        "job_owner",
        "project",
        "session_id",
        "queue",
        "server",
        "submit_arguments",
        "error_path",
        "output_path",
    ]

    rows = [(k, data.get(k, "(missing)")) for k in keys]

    table.clear(columns=False)
    table.add_rows(rows)


def update_properties(table, data):
    keys = [
        "priority",
        "rerunnable",
        "run_count",
        "checkpoint",
        "substate",
        "pset",
        "hold_types",
        "join_path",
        "keep_files",
        "mail_points",
    ]

    rows = [(k, data.get(k, "(missing)")) for k in keys]

    table.clear(columns=False)
    table.add_rows(rows)


def update_timestamps(table, data):
    def parse_timestamp(timestamp):
        if timestamp is None:
            return "(missing)"

        return dt.datetime.fromtimestamp(int(timestamp)).astimezone().isoformat()

    keys = [
        "ctime",
        "etime",
        "qtime",
        "stime",
        "mtime",
    ]

    rows = [(job_translations.get(k, k), parse_timestamp(data.get(k))) for k in keys]

    table.clear(columns=False)
    table.add_rows(rows)


def identity(k, v):
    return k, v


def update_execution(table, data):
    translator = {
        "job_state": lambda k, v: (k, job_states.get(v, v)),
    }

    keys = [
        "queue",
        "job_state",
        "exec_host",
        "exec_vnode",
        "jobdir",
        "comment",
    ]

    rows = [translator.get(k, identity)(k, data.get(k, "(missing)")) for k in keys]

    table.clear(columns=False)
    table.add_rows(rows)


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


def update_resources(table, data):
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
        "resource_list",
        "resources_used",
    ]
    resources = {
        key: {
            preprocess_group_key(group): data.get(group, {}).get(key, "(none)")
            for group in group_names
        }
        for key in expected_keys
    }
    translated = preprocess_resource_table(resources)
    rows = [
        (resource,) + tuple(group.values()) for resource, group in translated.items()
    ]
    table.clear(columns=False)
    table.add_rows(rows)


class JobDetailScreen(ModalScreen):
    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("ctrl+g", "refresh", "Refresh"),
        ("e", "environment", "Environment"),
        ("l", "logs", "Logs"),
    ]
    CSS_PATH = "job_details.tcss"

    def __init__(self, id, data):
        self._job_id = id
        self.data = data

        super().__init__()

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, new):
        translations = {
            "rerunable": "rerunnable",
        }

        data = dict(new)

        data["attributes"] = {
            translations.get(k.lower(), k.lower()): v
            for k, v in data.get("attributes", {}).items()
        }

        self._data = data

    def compose(self) -> ComposeResult:
        yield Header()

        yield Label(f"[b]Job: {self._job_id}[/b]", id="job_heading", classes="heading")

        # details:
        # - name, owner, project
        # - server, queue, vnode
        # - job_state, exit status, comment
        # - final log paths (error, output), if running: log paths on the node
        # - create / queue / finish time
        # - requested / used resources (including eligible time)
        # - properties:
        #   - priority
        #   - rerunnable
        #   - run_count
        #   - submit arguments
        #   - hold types / join path / keep files / mail points
        #   - checkpoint

        with Horizontal():
            with Container(classes="details", id="job_details"):
                yield Label(
                    "[i]Job details[/i]", id="details_heading", classes="heading"
                )
                yield DataTable(id="details", cursor_type="none")

            with Container(classes="details", id="properties"):
                yield Label(
                    "[i]Job properties[/i]", id="properties_heading", classes="heading"
                )
                yield DataTable(id="properties", zebra_stripes=True, cursor_type="none")

        with Horizontal():
            with Container(classes="details", id="execution"):
                with Container(id="execution_details"):
                    yield Label(
                        "[i]Execution environment[/i]",
                        classes="heading",
                        id="execution_heading",
                    )
                    yield DataTable(
                        id="execution", zebra_stripes=True, cursor_type="none"
                    )

                with Container(id="buttons"):
                    yield Button("environment", id="environment")
                    yield Button("logs", id="logs")

            with Container(classes="details", id="timestamps"):
                yield Label(
                    "[i]Timestamps[/i]", id="timestamps_heading", classes="heading"
                )
                yield DataTable(id="timestamps", cursor_type="none")

        with Container(classes="details", id="resources"):
            yield Label("[i]Resources[/i]", id="resources_heading", classes="heading")

            yield DataTable(id="resources", zebra_stripes=True, cursor_type="none")

        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        screens = {
            "environment": EnvironmentScreen,
            "logs": LogScreen,
        }
        screen_cls = screens[event.button.id]

        self.app.push_screen(screen_cls(self.data["attributes"]))

    def action_environment(self) -> None:
        self.app.push_screen(EnvironmentScreen(self.data["attributes"]))

    def action_logs(self) -> None:
        self.app.push_screen(LogScreen(self._data["attributes"]))

    def action_refresh(self) -> None:
        self.refresh_content()

    @work(exclusive=True)
    async def refresh_content(self) -> None:
        data = await query(self.app.config.local_port, kind="job")

        self.post_message(JobQueryResult(data=data))

        self.data = data[self._job_id]

        self.refresh_data()

    def refresh_data(self) -> None:
        job_details = self.query_one("DataTable#details")
        update_job_details(job_details, self.data["attributes"])

        properties = self.query_one("DataTable#properties")
        update_properties(properties, self.data["attributes"])

        timestamps = self.query_one("DataTable#timestamps")
        update_timestamps(timestamps, self.data["attributes"])

        execution = self.query_one("DataTable#execution")
        update_execution(execution, self.data["attributes"])

        resources = self.query_one("DataTable#resources")
        update_resources(resources, self.data["attributes"])

    def on_mount(self) -> None:
        job_details = self.query_one("DataTable#details")
        job_details.add_columns("name", "value")

        properties = self.query_one("DataTable#properties")
        properties.add_columns("name", "value")

        timestamps = self.query_one("DataTable#timestamps")
        timestamps.add_columns("name", "value")

        execution = self.query_one("DataTable#execution")
        execution.add_columns("name", "value")

        resources = self.query_one("DataTable#resources")
        resources.add_columns("name", "requested", "used")

        self.refresh_data()


class EnvironmentScreen(ModalScreen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    CSS_PATH = "job_modal.tcss"

    def __init__(self, data):
        super().__init__()

        self._data = data

    def compose(self) -> ComposeResult:
        with Container(id="content"):
            yield Label("[i]Environment variables[/i]", classes="heading")

            yield DataTable(id="environment", cursor_type="none", zebra_stripes=True)

            yield Button("Quit")

    def on_mount(self):
        table = self.query_one("DataTable#environment")
        table.add_columns("name", "value")

        variables = self._data["variable_list"]

        rows = [(name, value) for name, value in variables.items()]
        table.add_rows(rows)

    def on_button_pressed(self, event: Button.Pressed):
        self.app.pop_screen()


class LogScreen(ModalScreen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    CSS_PATH = "job_modal.tcss"

    def __init__(self, data):
        super().__init__()

        self._data = data

    def compose(self) -> ComposeResult:
        from textual.widgets import Placeholder

        yield Label("[i]Log files[/i]", classes="heading")
        with Horizontal(id="content"):
            with Container(id="output"):
                yield Label("[i]standard output[/i]", classes="heading")

                yield Placeholder("stdout")

            with Container(id="error"):
                yield Label("[i]standard error[/i]", classes="heading")

                yield Placeholder("stderr")

        yield Button("Quit")

    def on_button_pressed(self, event: Button.Pressed):
        self.app.pop_screen()
