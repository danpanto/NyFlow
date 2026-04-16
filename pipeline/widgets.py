from rich.text import Text
from textual import events
from textual.widget import Widget
from textual.widgets import Log
from textual.widgets import Tabs as TxtTabs
from textual.reactive import reactive
from textual.message import Message


class OptionBox(Widget):
    can_focus = True
    value = reactive("", layout=True)

    class Changed(Message):
        def __init__(self, sender: "OptionBox", value: str) -> None:
            super().__init__()
            self.sender = sender
            self.value = value

    def __init__(
        self, options: list[str], id: str | None = None, classes: str | None = None
    ):
        super().__init__(id=id, classes=classes)
        self.options = options
        self._index = 0
        self.value = options[0]

    def render(self) -> Text:
        label = str(self.value)
        text = f"[ {label} ]"

        if self.has_focus:
            text = f"< {label} >"

        return Text(text)

    def watch_value(self, new_value: str) -> None:
        self.styles.width = len(new_value) + 4
        self.post_message(self.Changed(self, new_value))
        self.refresh()

    def on_key(self, event: events.Key) -> None:
        if event.key == "right":
            self._index = (self._index + 1) % len(self.options)
        elif event.key == "left":
            self._index = (self._index - 1) % len(self.options)
        else:
            return

        self.value = self.options[self._index]

    def on_focus(self) -> None:
        self.refresh()

    def on_blur(self) -> None:
        self.refresh()


class CheckBox(Widget):
    can_focus = True
    value = reactive(False, layout=True)

    class Changed(Message):
        def __init__(self, sender: "CheckBox", value: bool) -> None:
            super().__init__()
            self.sender = sender
            self.value = value

    def __init__(
        self,
        is_selected: bool = False,
        id: str | None = None,
        classes: str | None = None,
    ):
        super().__init__(id=id, classes=classes)
        self.value = is_selected
        self.styles.width = 3

    def render(self) -> Text:
        return Text("[X]" if self.value else "[ ]")

    def on_key(self, event: events.Key) -> None:
        if event.key in ("space", "enter"):
            self.value = not self.value
            self.refresh()

    def watch_value(self, new_value: bool) -> None:
        self.post_message(self.Changed(self, new_value))
        self.refresh()

    def on_focus(self) -> None:
        self.refresh()

    def on_blur(self) -> None:
        self.refresh()


class Button(Widget):
    can_focus = True

    def __init__(
        self, text: str, action, id: str | None = None, classes: str | None = None
    ):
        super().__init__(id=id, classes=classes)
        self.text = text
        self.styles.width = 2 + len(text)
        self.action = action

    def render(self) -> Text:
        return Text(f"<{self.text}>")

    def on_key(self, event: events.Key) -> None:
        if event.key in ("space", "enter"):
            self.action()
            self.refresh()

    def on_focus(self) -> None:
        self.refresh()

    def on_blur(self) -> None:
        self.refresh()


class Tabs(TxtTabs):
    can_focus = False


class LogView(Log):
    STATUS_COLORS = {
        "SUCCESS": "green",
        "ERROR": "red",
        "WARNING": "yellow",
        "INFO": "cyan",
    }

    def add_line(self, message: str, status: str = "INFO") -> None:
        from datetime import datetime

        # 1. Formatting
        ts = datetime.now().strftime("%H:%M:%S")
        color = self.STATUS_COLORS.get(status.upper(), "white")

        # 2. Build the markup string
        # {status:7} ensures the text after the status aligns vertically
        formatted = f"[{ts}] {f'[{status}]':9} {message}"

        # 3. Use write_line with markup=True
        self.write_line(formatted)
