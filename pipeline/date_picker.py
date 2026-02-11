from pipeline.widgets import Button
from pipeline.selection_tree import SelectionTree
from textual import events
from textual.app import ComposeResult
from textual.screen import ModalScreen
from textual.containers import Middle, Center, Horizontal, Vertical
from textual.widgets import Label, SelectionList


class DatePickerModal(ModalScreen):

    CSS_PATH = "style.tcss"
    
    def __init__(self, dates: dict, selected_dates: set = None):
        super().__init__()
        self.dates = dates
        self.selected_dates = selected_dates


    def confirm(self):
        self.dismiss(self.query_one("#date-tree").get_selected_values())

    
    def on_key(self, event: events.Key) -> None:
        date_tree = self.query_one("#date-tree")
        confirm_btn = self.query_one("#confirm-btn")
        cancel_btn = self.query_one("#cancel-btn")

        if event.key == "right":
            if self.focused == confirm_btn:
                cancel_btn.focus()
            elif self.focused == cancel_btn:
                confirm_btn.focus()
            event.stop()

        elif event.key == "left":
            if self.focused == cancel_btn:
                confirm_btn.focus()
            elif self.focused == confirm_btn:
                cancel_btn.focus()
            event.stop()

        elif event.key == "tab":
            if self.focused == date_tree:
                confirm_btn.focus()
            elif self.focused in (confirm_btn, cancel_btn):
                date_tree.focus()
            event.stop()


    def compose(self) -> ComposeResult:
        with Middle():
            with Center():
                with Vertical(id="modal-dialog"):
                    yield Label("Select Data Range", id="modal-title")
                    
                    # This horizontal container holds the two lists side-by-side
                    yield SelectionTree(self.dates, self.selected_dates, id="date-tree", start_expanded=False)

                    with Horizontal(id="modal-footer"):
                        yield Button("Cancel", action=lambda: self.dismiss(None), id="cancel-btn", classes="focuseable")
                        yield Button("Confirm", action=self.confirm, id="confirm-btn", classes="focuseable")
