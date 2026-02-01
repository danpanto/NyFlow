from pipeline.widgets import Button
from textual import events
from textual.app import ComposeResult
from textual.screen import ModalScreen
from textual.containers import Middle, Center, Horizontal, Vertical
from textual.widgets import Label, SelectionList


class DatePickerModal(ModalScreen):

    CSS_PATH = "style.tcss"
    
    def __init__(self, years, months, selected_years=None, selected_months=None):
        super().__init__()
        self.years = years
        self.months = months
        self.last_button = None
        self.last_group = None
        self.selected_years = selected_years or []
        self.selected_months = selected_months or []


    def on_mount(self) -> None:
        """Initialize selections after the widgets are mounted."""
        year_list = self.query_one("#year-list", SelectionList)
        for y in self.selected_years:
            if y in self.years:
                year_list.select(y)

        month_list = self.query_one("#month-list", SelectionList)
        for m in self.selected_months:
            if m in self.months:
                month_list.select(m)


    def confirm(self):
        results = {
            "years": self.query_one("#year-list").selected,
            "months": self.query_one("#month-list").selected
        }
        self.dismiss(results)

    
    def on_key(self, event: events.Key) -> None:
        year_list = self.query_one("#year-list")
        month_list = self.query_one("#month-list")
        confirm_btn = self.query_one("#confirm-btn")
        cancel_btn = self.query_one("#cancel-btn")

        # --- HORIZONTAL NAVIGATION ---
        if event.key == "right":
            if self.focused == year_list:
                month_list.focus()
                event.stop()
            elif self.focused == month_list:
                year_list.focus()
                event.stop()
            elif self.focused == confirm_btn:
                cancel_btn.focus()
                event.stop()
            elif self.focused == cancel_btn:
                confirm_btn.focus()
                event.stop()

        elif event.key == "left":
            if self.focused == month_list:
                year_list.focus()
                event.stop()
            elif self.focused == year_list:
                month_list.focus()
                event.stop()
            elif self.focused == cancel_btn:
                confirm_btn.focus()
                event.stop()
            elif self.focused == confirm_btn:
                cancel_btn.focus()
                event.stop()

        # --- GROUP JUMPING ---
        elif event.key == "tab":
            if self.focused in (year_list, month_list):
                if self.last_button is None:
                    cancel_btn.focus()
                else:
                    self.last_button.focus()
                self.last_group = self.focused
                event.stop()
            elif self.focused in (confirm_btn, cancel_btn):
                if self.last_group is None:
                    year_list.focus()
                else:
                    self.last_group.focus()
                self.last_button = self.focused
                event.stop()
        
        elif event.key in ("up", "down"):
            if self.focused in (confirm_btn, cancel_btn):
                event.stop()


    def compose(self) -> ComposeResult:
        with Middle():
            with Center():
                with Vertical(id="modal-dialog"):
                    yield Label("Select Data Range", id="modal-title")
                    
                    # This horizontal container holds the two lists side-by-side
                    with Horizontal(id="lists-container"):
                        with Vertical(classes="list-column"):
                            yield Label("Years")
                            yield SelectionList(*[(y, y) for y in self.years], id="year-list")
                        
                        with Vertical(classes="list-column"):
                            yield Label("Months")
                            yield SelectionList(*[(m, m) for m in self.months], id="month-list")

                    with Horizontal(id="modal-footer"):
                        yield Button("Cancel", action=lambda: self.dismiss(None), id="cancel-btn", classes="focuseable")
                        yield Button("Confirm", action=self.confirm, id="confirm-btn", classes="focuseable")
