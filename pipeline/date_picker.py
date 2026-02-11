from pipeline.widgets import Button
from pipeline.selection_tree import SelectionTree
from textual import events
from textual.app import ComposeResult
from textual.screen import ModalScreen
from textual.containers import Middle, Center, Horizontal, Vertical
from textual.widgets import Label, SelectionList


class DatePickerModal(ModalScreen):

    CSS_PATH = "style.tcss"
    
    def __init__(self, years, months, selected_years=None, selected_months=None):
        super().__init__()


    def confirm(self):
        self.dismiss(None)

    
    def on_key(self, event: events.Key) -> None:
        date_tree = self.query_one("#date-tree")
        confirm_btn = self.query_one("#confirm-btn")
        cancel_btn = self.query_one("#cancel-btn")

        if event.key == "right":
            if self.focused == confirm_btn:
                cancel_btn.focus()
            elif self.focused == cancel_btn:
                confirm_btn.focus()

        elif event.key == "left":
            if self.focused == cancel_btn:
                confirm_btn.focus()
            elif self.focused == confirm_btn:
                cancel_btn.focus()

        elif event.key == "tab":
            if self.focused == date_tree:
                confirm_btn.focus()
            elif self.focused in (confirm_btn, cancel_btn):
                date_tree.focus()
        
        if event.key in ("left", "right", "tab"):
            event.stop()


    def compose(self) -> ComposeResult:
        with Middle():
            with Center():
                with Vertical(id="modal-dialog"):
                    yield Label("Select Data Range", id="modal-title")
                    
                    # This horizontal container holds the two lists side-by-side
                    data = {
                        "2025": {
                            "Jan": "2025-01",
                            "Feb": "2025-02"
                        },
                        "2024": {
                            "Nov": "2024-11",
                            "Dec": "2024-12"
                        },
                    }
                    yield SelectionTree(data, id="date-tree")

                    with Horizontal(id="modal-footer"):
                        yield Button("Cancel", action=lambda: self.dismiss(None), id="cancel-btn", classes="focuseable")
                        yield Button("Confirm", action=self.confirm, id="confirm-btn", classes="focuseable")
