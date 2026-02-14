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
        self.selected_dates = set(selected_dates) if selected_dates else set()
        self.last_button = None


    def confirm(self):
        self.dismiss(self.query_one("#date-tree").get_selected_values())


    def cancel(self):
        self.dismiss(None)

    
    def on_key(self, event: events.Key) -> None:
        if event.key == "escape":
            event.stop()
            self.cancel()

        date_tree = self.query_one("#date-tree")
        confirm_btn = self.query_one("#confirm-btn")
        cancel_btn = self.query_one("#cancel-btn")
        modal_sidebar = self.query_one("#modal-sidebar")

        if event.key == "left":
            if self.focused != date_tree:
                self.last_button = self.focused
                date_tree.focus()
            event.stop()

        elif event.key == "right":
            if self.focused == date_tree:
                if self.last_button:
                    self.last_button.focus()
                else:
                    confirm_btn.focus()
            event.stop()

        elif event.key == "up":
            if self.focused != modal_sidebar.children[0]:
                self.focus_previous()
            event.stop()

        elif event.key == "down":
            if self.focused != modal_sidebar.children[-1]:
                self.focus_next()
            event.stop()


    def compose(self) -> ComposeResult:
        with Middle():
            with Center():
                with Vertical(id="modal-dialog"):
                    yield Label("Select Data Range", id="modal-title")

                    with Horizontal(id="main-container"):
                        yield SelectionTree(self.dates, self.selected_dates, id="date-tree", start_expanded=False)

                        with Vertical(id="modal-sidebar"):
                            yield Button("Cancel", action=self.cancel, id="cancel-btn", classes="focuseable")
                            yield Button("Confirm", action=self.confirm, id="confirm-btn", classes="focuseable")
