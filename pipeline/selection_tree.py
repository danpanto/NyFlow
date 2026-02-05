from rich.text import Text
from textual.widgets import Tree
from pathlib import Path

class SelectionTree(Tree):
    """A Tree that builds itself using the Path API."""

    def __init__(self, paths: list[Path], selected: list[Path] | None = None, **kwargs):
        # We use a generic "Data" root label
        super().__init__("Data", **kwargs)
        self.show_root = False
        self.all_paths = paths
        self.selected_paths = set(selected or [])
        self.build()

    def _get_checkbox(self, path: Path) -> Text:
        """Standard [X] or [ ] render logic."""
        is_selected = path in self.selected_paths
        return Text.assemble(
            ("[", "white"),
            ("X" if is_selected else " ", "deep_sky_blue1" if is_selected else "white", "bold"),
            ("]", "white")
        )

    def build(self) -> None:
        self.clear()
        # Ensure paths are sorted for consistent tree building
        for path in sorted(self.all_paths):
            current_node = self.root
            # 'parts' gives us a tuple: ('2024', 'Jan', 'file.parquet')
            parts = path.parts
            
            accumulated_path = Path()
            for i, part in enumerate(parts):
                accumulated_path = accumulated_path / part
                is_last = (i == len(parts) - 1)

                # Find if this specific segment already exists in the current branch
                target_node = None
                for child in current_node.children:
                    if child.data and child.data.get("full_path") == accumulated_path:
                        target_node = child
                        break
                
                if target_node is None:
                    if is_last:
                        # It's a file leaf
                        label = Text.assemble(self._get_checkbox(path), f" {part}")
                        current_node.add_leaf(label, data={"full_path": path, "is_file": True})
                    else:
                        # It's a directory branch
                        current_node = current_node.add(
                            Text(part, style="bold"), 
                            data={"full_path": accumulated_path, "is_file": False}, 
                            expand=True
                        )
                else:
                    # Move deeper into the existing folder
                    current_node = target_node

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        if node.data and node.data.get("is_file"):
            path = node.data["full_path"]
            
            if path in self.selected_paths:
                self.selected_paths.remove(path)
            else:
                self.selected_paths.add(path)
            
            # Update the label using the last part of the Path (the filename)
            node.label = Text.assemble(self._get_checkbox(path), f" {path.name}")
            node.refresh()

    @property
    def selected(self) -> list[Path]:
        return list(self.selected_paths)
