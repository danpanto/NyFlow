from textual.widgets import Tree, Label
from textual.widgets.tree import TreeNode
from textual.containers import Middle, Center, Horizontal, Vertical
from textual import events
from textual.app import ComposeResult
from textual.screen import ModalScreen
from pipeline.widgets import Button
from minio_utils import MinioSparkClient


class SelectionTree(Tree):
    """
    A recursive tree that builds itself from a nested dictionary.
    Supports multi-selection of leaf nodes.
    """

    can_focus = True

    def __init__(self, data: dict, selected_data: set | None = None, label: str = "Root",
                    id: str | None = None, start_expanded: bool = False):

        super().__init__(label, id=id)
        self.node_data = data
        self.selected_data = selected_data if selected_data is not None else set()
        self.start_expanded = start_expanded


    def _build_tree(self, data: dict, parent_node: TreeNode):
        """Recursively adds nodes based on dictionary depth."""
        for key, value in data.items():
            if isinstance(value, dict):
                # It's a branch
                node = parent_node.add(str(key), expand=self.start_expanded, data={"num_checked": 0})
                self._build_tree(value, node)
                if parent_node.data is not None and "num_checked" in parent_node.data:
                    parent_node.data["num_checked"] += node.data["num_checked"]
            else:
                # It's a leaf
                is_selected = value in self.selected_data
                label = self._format_label(str(key), is_selected)
                parent_node.add_leaf(label, data={"val": value, "key": key, "checked": is_selected})
                if parent_node.data is not None and "num_checked" in parent_node.data:
                    parent_node.data["num_checked"] += int(is_selected)


    def _format_label(self, text: str, is_selected: bool) -> str:
        icon = "[X]" if is_selected else "[ ]"
        return f"{icon} {text}"


    def _is_leaf(self, node: TreeNode):
        return not node.allow_expand


    def _recursive_leaf_toggle(self, node: TreeNode, select: bool) -> int:
        if not self._is_leaf(node):
            delta = 0
            for child in node.children:
                delta += self._recursive_leaf_toggle(child, select)
            node.data["num_checked"] += delta
            return delta

        if node.data["checked"] == select:
            return 0

        node.data["checked"] = select
        if select:
            self.selected_data.add(node.data["val"])
            node.set_label(self._format_label(node.data["key"], True))
            return 1
        else:
            self.selected_data.discard(node.data["val"])
            node.set_label(self._format_label(node.data["key"], False))
            return -1


    def rebuild(self, new_data):
        self.node_data = new_data 
        
        self.clear()
        
        self.selected_data.clear() 

        self._build_tree(self.node_data, self.root)
    
        if self.start_expanded:
            self.root.expand_all()
        
        if self.root.children:
            self.move_cursor(self.root.children[0])
        
        self.refresh()


    def on_mount(self):
        self.show_root = False
        self._build_tree(self.node_data, self.root)

        self.root.expand_all() if self.start_expanded else self.root.collapse_all()

        if len(self.root.children) > 0:
            self.move_cursor(self.root.children[0])
            self.cursor_node.toggle()  #type:ignore


    def handle_selection(self, node):
        """Unified logic for toggling branches or checking leaves."""
        
        if not self._is_leaf(node):
            node.toggle()
        else:
            val = node.data["val"]
            node.data["checked"] = not node.data["checked"]

            if node.data["checked"]:
                self.selected_data.add(val)
                node.parent.data["num_checked"] += 1
            else:
                self.selected_data.remove(val)
                node.parent.data["num_checked"] -= 1

            node.set_label(self._format_label(node.data["key"], node.data["checked"]))


    def on_tree_node_selected(self, event: Tree.NodeSelected):
        """Handle the Enter key (default selection event)."""
        self.handle_selection(event.node)


    def get_selected_values(self) -> set:
        """Returns the 'value' portion of all selected leaf nodes."""
        return self.selected_data


    def on_key(self, event: events.Key):
        if event.key == "up":
            self.action_cursor_up()
            event.stop()

        elif event.key == "down":
            self.action_cursor_down()
            event.stop()

        elif event.key in ("space", "enter"):
            if self.cursor_node:
                self.handle_selection(self.cursor_node)
                event.stop()

        elif event.key == "shift+enter":
            parent = self.cursor_node.parent if self._is_leaf(self.cursor_node) else self.cursor_node  #type:ignore
            self._recursive_leaf_toggle(parent, parent.data["num_checked"] < len(parent.children))
            event.stop()

        elif event.key in ("tab", "shift+tab"):
            new_focus = self.cursor_node.parent if self._is_leaf(self.cursor_node) else self.cursor_node  #type:ignore

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
                
                new_focus.toggle()  #type:ignore
                event.stop()



class TreeSelectionModal(ModalScreen):

    CSS_PATH = "style.tcss"
    
    def __init__(self, data: dict, selected_data: set | None = None,
        title_text: str = "Select Data", client: MinioSparkClient | None = None):

        super().__init__()
        self.data = data
        self.selected_data = set(selected_data) if selected_data else set()
        self.last_button = None
        self.title_text = title_text
        self._client = client


    def confirm(self):
        self.dismiss(self.query_one("#selection-tree").get_selected_values())  #type:ignore


    def cancel(self):
        self.dismiss(None)

    
    def on_key(self, event: events.Key) -> None:
        from pipeline.pl_utils import get_parquet_files

        if event.key == "escape":
            event.stop()
            self.cancel()

        selection_tree = self.query_one("#selection-tree")
        confirm_btn = self.query_one("#confirm-btn")
        modal_sidebar = self.query_one("#modal-sidebar")

        if event.key == "r":
            selection_tree.rebuild(get_parquet_files(self._client))  #type:ignore
            event.stop()

        elif event.key == "left":
            if self.focused != selection_tree:
                self.last_button = self.focused
                selection_tree.focus()
            event.stop()

        elif event.key == "right":
            if self.focused == selection_tree:
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
                    yield Label(self.title_text, id="modal-title")

                    with Horizontal(id="main-container"):
                        yield SelectionTree(
                            self.data,
                            self.selected_data,
                            id="selection-tree",
                            start_expanded=False
                        )

                        with Vertical(id="modal-sidebar"):
                            yield Button("Cancel", action=self.cancel, id="cancel-btn", classes="focuseable")
                            yield Button("Confirm", action=self.confirm, id="confirm-btn", classes="focuseable")
