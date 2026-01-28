from textual.app import App, ComposeResult
from textual.containers import Center, Middle, Vertical, Horizontal
from textual.widgets import Label, Button
from optionBox import OptionBox
from textual import events

class Pipeline(App):
    CSS_PATH = "style.tcss"

    def compose(self) -> ComposeResult:
        with Middle():
            with Center():
                with Vertical(id="dialog"):
                    yield Label("Download Pipeline Settings", id="title")
                    
                    with Horizontal(classes="optbox-row"):
                        yield Label("Download Mode:")
                        yield OptionBox(
                            ["Everything", "Missing only", "Custom"],
                            id="dl_selector"
                        )

                    with Horizontal(classes="optbox-row"):
                        yield Label("Transformations:")
                        yield OptionBox(
                            ["None", "Outliers", "Add/Del columns", "All"],
                            id="tf_selector"
                        )
                    
                    with Horizontal(classes="dl_exit-row"):
                        yield Button("Exit", variant="error")
                        yield Button("Download", variant="success")

    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.variant == "success":
            # Logic for OK
            self.exit()
        else:
            self.exit()


    def on_key(self, event: events.Key) -> None:
        if event.key == "up":
            self.screen.focus_previous()
        elif event.key == "down":
            self.screen.focus_next()



if __name__ == '__main__':
    Pipeline().run()

