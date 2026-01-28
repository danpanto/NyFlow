from textual.widget import Widget, Text
from textual.reactive import reactive
from textual.message import Message
from textual import events

class OptionBox(Widget):
    """A clean, reactive text widget for cycling options."""

    can_focus = True
    value = reactive("", layout=True)


    class Changed(Message):
        """Posted when the value changes."""
        def __init__(self, value: str) -> None:
            super().__init__()
            self.value = value


    def __init__(self, options: list[str], id: str | None = None):
        super().__init__(id=id)
        self.options = options
        self._index = 0
        self.value = options[0]


    def render(self) -> str:
        label = str(self.value)
        text= f"[ {label} ]"
        
        if self.has_focus:
            text = f"< {label} >"

        return Text(text)
        


    def watch_value(self, new_value: str) -> None:
        self.styles.width = len(new_value) + 4
        self.post_message(self.Changed(new_value))
        self.refresh()


    def on_key(self, event: events.Key) -> None:
        """Handle navigation keys."""
        if event.key == "right":
            self._index = (self._index + 1) % len(self.options)
        elif event.key == "left":
            self._index = (self._index - 1) % len(self.options)
        else:
            return # Don't update if it's another key

        self.value = self.options[self._index]


    def on_focus(self) -> None:
        self.refresh()


    def on_blur(self) -> None:
        self.refresh()
