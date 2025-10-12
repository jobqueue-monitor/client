import json
import pathlib
import re

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Input, Static

from jobqueue_monitor.utils import natural_sort_key, translate_json

path = pathlib.Path(__file__).parent / "../../../dummy-server/qstat_job.json"


def query_data(path):
    return translate_json(json.loads(path.read_text()))["Jobs"]


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


def deep_match(expr, value):
    match value:
        case dict() as obj:
            return any(deep_match(expr, v) for v in obj.values())
        case list() as obj:
            return any(deep_match(expr, v) for v in obj)
        case str() as obj:
            return expr.match(obj) is not None
        case _ as obj:
            return expr.match(str(obj)) is not None


def extract_row(id, attrs):
    job_state = attrs["job_state"]
    return (
        id,
        attrs["queue"],
        job_states.get(job_state, job_state),
        attrs["Job_Name"],
        attrs["Job_Owner"],
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
            yield Static("[i]Jobs[/i]", id="jobs_heading", classes="heading")
            yield Input(
                placeholder="Search jobs",
                type="text",
                id="job_search_bar",
                select_on_focus=True,
            )

            with Container():
                yield DataTable(
                    classes="jobs_table", cursor_type="row", zebra_stripes=True
                )
        yield Footer()

    def action_refresh(self):
        table = self.query_one(DataTable)

        self.data = query_data(path)
        update_job_table(table, self.data)

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns("id", "queue", "status", "name", "owner", "walltime")
        table.focus()

        self.action_refresh()

    def on_input_changed(self, event: Input.Changed) -> None:
        search_bar = self.query_one(Input)
        table = self.query_one(DataTable)

        expression_re = re.compile(search_bar.value)

        new_data = {
            key: value
            for key, value in self.data.items()
            if expression_re.match(key) is not None or deep_match(expression_re, value)
        }
        update_job_table(table, new_data)

    def on_input_submitted(self):
        table = self.query_one(DataTable)

        table.focus()

    def action_search(self):
        search_bar = self.query_one(Input)

        search_bar.focus()
