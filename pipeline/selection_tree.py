from textual.widgets import Tree
from textual.widgets.tree import TreeNode
from textual import events

class SelectionTree(Tree):
    """
    A recursive tree that builds itself from a nested dictionary.
    Supports multi-selection of leaf nodes.
    """

    can_focus = True

    def __init__(self, data: dict, label: str = "Root", id: str | None = None):
        super().__init__(label, id=id)
        self.data_structure = data
        self.selected_paths = set()


    def _build_tree(self, data: dict, parent_node: TreeNode):
        """Recursively adds nodes based on dictionary depth."""
        for key, value in data.items():
            if isinstance(value, dict):
                # It's a branch
                node = parent_node.add(str(key), expand=True)
                self._build_tree(value, node)
            else:
                # It's a leaf (selectable item)
                # We store the 'value' in node.data for the caller to retrieve
                label = self._format_label(str(key), False)
                parent_node.add_leaf(label, data={"val": value, "key": key})


    def _format_label(self, text: str, is_selected: bool) -> str:
        icon = "[X]" if is_selected else "[ ]"
        return f"{icon} {text}"


    def on_key(self, event: events.Key) -> None:
        """Handle your custom toggling."""
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


    def on_mount(self) -> None:
        self.show_root = False
        # Start the recursive build from the root
        self._build_tree(self.data_structure, self.root)


    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle the Enter key (default selection event)."""
        self.handle_selection(event.node)


    def handle_selection(self, node) -> None:
        """Unified logic for toggling branches or checking leaves."""
        if node.allow_expand:
            node.toggle()
        else:
            item_id = id(node)
            if item_id in self.selected_paths:
                self.selected_paths.remove(item_id)
                node.set_label(self._format_label(node.data["key"], False))
            else:
                self.selected_paths.add(item_id)
                node.set_label(self._format_label(node.data["key"], True))


    def get_selected_values(self) -> list:
        """Returns the 'value' portion of all selected leaf nodes."""
        selected = []
        for node in self.find_nodes(lambda n: not n.allow_expand):
            if id(node) in self.selected_paths:
                selected.append(node.data["val"])
        return selected
