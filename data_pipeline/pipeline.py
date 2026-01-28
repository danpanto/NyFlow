from textual.app import App
from textual.containers import Center, Middle, Vertical, Horizontal
from textual.widgets import Label
from textual import events
from widgets import *

class Pipeline(App):
    CSS_PATH = "style.tcss"

    def compose(self):
        with Middle():
            with Center():
                with Vertical(id="dialog"):
                    yield Label("Download Pipeline Settings", id="title")

                    with Horizontal(classes="optbox-row"):
                        yield Label("Transformations:")
                        yield OptionBox(
                            ["None", "Outliers", "Add/Del columns", "All"],
                            id="tf_selector",
                            classes="focuseable"
                        )
                    
                    with Horizontal(classes="optbox-row"):
                        yield Label("Vendors:")
                        yield OptionBox(
                            ["All", "Custom"],
                            id="dl_selector",
                            classes="focuseable"
                        )

                    with Vertical(id="vendors-collapsable"):
                        with Horizontal(classes="optbox_sub1-row"):
                            yield Label("Yellow Taxi")
                            yield OptionBox(["All", "Missing only"], id="yellow_selector", classes="focuseable")

                        with Horizontal(classes="optbox_sub1-row"):
                            yield Label("Green Taxi")
                            yield OptionBox(["All", "Missing only"], id="green_selector", classes="focuseable")

                        with Horizontal(classes="optbox_sub1-row"):
                            yield Label("For-Hire Vehicle")
                            yield OptionBox(["All", "Missing only"], id="for-hire_selector", classes="focuseable")

                        with Horizontal(classes="optbox_sub1-row"):
                            yield Label("High Volume For-Hire Vehicle")
                            yield OptionBox(["All", "Missing only"], id="high-volume_selector", classes="focuseable")
                    
                    with Vertical(classes="down-right"):
                        yield Button("Download", action=self.exit, classes="focuseable")
                        yield Button("Exit", action=self.exit, classes="focuseable")


    def on_key(self, event: events.Key) -> None:
        if event.key == "up":
            self.screen.focus_previous()
        elif event.key == "down":
            self.screen.focus_next()

    
    def on_option_box_changed(self, message: OptionBox.Changed) -> None:
        if message.sender.id == "dl_selector":
            container = self.query_one("#vendors-collapsable")
            if message.value == "Custom":
                container.display = True
            else:
                container.display = False



if __name__ == '__main__':
    Pipeline().run()

