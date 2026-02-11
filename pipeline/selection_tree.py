from textual.widgets import Tree
from textual.widgets.tree import TreeNode
from textual import events

class SelectionTree(Tree):
    """
    A recursive tree that builds itself from a nested dictionary.
    Supports multi-selection of leaf nodes.
    """

    can_focus = True

    def __init__(self, data: dict, selected_data: set = None, label: str = "Root",
                    id: str | None = None, start_expanded: bool = False
                ):

        super().__init__(label, id=id)
        self.node_data = data
        self.selected_data = selected_data if selected_data is not None else set()
        self.start_expanded = start_expanded


    def _build_tree(self, data: dict, parent_node: TreeNode):
        """Recursively adds nodes based on dictionary depth."""
        for key, value in data.items():
            if isinstance(value, dict):
                # It's a branch
                node = parent_node.add(str(key), expand=self.start_expanded)
                self._build_tree(value, node)
            else:
                # It's a leaf
                label = self._format_label(str(key), value in self.selected_data)
                parent_node.add_leaf(label, data={"val": value, "key": key})


    def _format_label(self, text: str, is_selected: bool) -> str:
        icon = "[X]" if is_selected else "[ ]"
        return f"{icon} {text}"


    def on_key(self, event: events.Key):
        if event.key == "space":
            if self.cursor_node:
                self.handle_selection(self.cursor_node)
                event.stop()

        elif event.key == "up":
            self.action_cursor_up()
            event.stop()

        elif event.key == "down":
            self.action_cursor_down()
            event.stop()

        elif event.key in ("tab", "shift+tab"):
            new_focus = self.cursor_node if self.cursor_node.allow_expand else self.cursor_node.parent

            if event.key == "shift+tab":
                if any(child.is_expanded for child in self.root.children):
                    self.root.collapse_all()
                    self.root.expand()
                else:
                    self.root.expand_all()

                if self.root.children:
                    self.move_cursor(self.root.children[0])
                
                event.stop()

            elif new_focus != self.root:
                if new_focus != self.cursor_node:
                    self.move_cursor(new_focus)
                
                new_focus.toggle()
                event.stop()
            


    def on_mount(self):
        self.show_root = False
        self._build_tree(self.node_data, self.root)

        self.root.expand_all() if self.start_expanded else self.root.collapse_all()
        self.move_cursor(self.root.children[0])
        self.cursor_node.toggle()


    def on_tree_node_selected(self, event: Tree.NodeSelected):
        """Handle the Enter key (default selection event)."""
        self.handle_selection(event.node)


    def handle_selection(self, node):
        """Unified logic for toggling branches or checking leaves."""
        if node.allow_expand:
            node.toggle()
        else:
            val = node.data["val"]
            if val in self.selected_data:
                self.selected_data.remove(val)
                node.set_label(self._format_label(node.data["key"], False))
            else:
                self.selected_data.add(val)
                node.set_label(self._format_label(node.data["key"], True))


    def get_selected_values(self) -> set:
        """Returns the 'value' portion of all selected leaf nodes."""
        return self.selected_data
