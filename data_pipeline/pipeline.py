from textual.app import App
from textual.containers import Center, Middle, Vertical, Horizontal
from textual.widgets import Label, SelectionList
from textual import events, work
from textual.screen import ModalScreen
from data_pipeline.widgets import *


def get_years_months_vendors() -> tuple[list[str], list[str], list[str]] | None:
    from bs4 import BeautifulSoup
    import requests as rq

    response = rq.get("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    if response.status_code // 100 != 2:
        print(f"[Error] Status code: {response.status_code}")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")

    # Get available years
    years = [tag.get("id")[3:] for tag in soup.find_all("div", {"class": "faq-answers"})]

    
    months = list()
    vendors = list()
    for td in soup.find_all("td"):
        # Get available months
        for strong in td.find_all("strong"):
            m = strong.get_text(strip=True)
            if m not in months:
                months.append(m)

        # Get available vendors
        for link in td.find_all("a"):
            v = link.get("title")[:-13]
            if v not in vendors:
                vendors.append(v)

    return (years, months, vendors)



class DatePickerModal(ModalScreen):
    """A pop-up to select years and months."""

    CSS_PATH = "style.tcss"
    
    def __init__(self, years, months):
        super().__init__()
        self.years = years
        self.months = months
        self.last_button = None
        self.last_group = None


    def compose(self):
        with Middle():
            with Center():
                with Vertical(id="modal-dialog"):
                    yield Label("Select Data Range", id="modal-title")
                    
                    # This horizontal container holds the two lists side-by-side
                    with Horizontal(id="lists-container"):
                        with Vertical(classes="list-column"):
                            yield Label("Years")
                            yield SelectionList(*[(y, y) for y in self.years], id="year-list")
                        
                        with Vertical(classes="list-column"):
                            yield Label("Months")
                            yield SelectionList(*[(m, m) for m in self.months], id="month-list")

                    with Horizontal(id="modal-footer"):
                        yield Button("Cancel", action=lambda: self.dismiss(None), id="cancel-btn", classes="focuseable")
                        yield Button("Confirm", action=self.confirm, id="confirm-btn", classes="focuseable")


    def confirm(self):
        results = {
            "years": self.query_one("#year-list").selected,
            "months": self.query_one("#month-list").selected
        }
        self.dismiss(results)

    
    def on_key(self, event: events.Key) -> None:
        year_list = self.query_one("#year-list")
        month_list = self.query_one("#month-list")
        confirm_btn = self.query_one("#confirm-btn")
        cancel_btn = self.query_one("#cancel-btn")

        # --- HORIZONTAL NAVIGATION ---
        if event.key == "right":
            if self.focused == year_list:
                month_list.focus()
                event.stop()
            elif self.focused == month_list:
                year_list.focus()
                event.stop()
            elif self.focused == confirm_btn:
                cancel_btn.focus()
                event.stop()
            elif self.focused == cancel_btn:
                confirm_btn.focus()
                event.stop()

        elif event.key == "left":
            if self.focused == month_list:
                year_list.focus()
                event.stop()
            elif self.focused == year_list:
                month_list.focus()
                event.stop()
            elif self.focused == cancel_btn:
                confirm_btn.focus()
                event.stop()
            elif self.focused == confirm_btn:
                cancel_btn.focus()
                event.stop()

        # --- GROUP JUMPING ---
        elif event.key == "tab":
            if self.focused in (year_list, month_list):
                if self.last_button is None:
                    cancel_btn.focus()
                else:
                    self.last_button.focus()
                self.last_group = self.focused
                event.stop()
            elif self.focused in (confirm_btn, cancel_btn):
                if self.last_group is None:
                    year_list.focus()
                else:
                    self.last_group.focus()
                self.last_button = self.focused
                event.stop()
        
        elif event.key in ("up", "down"):
            if self.focused in (confirm_btn, cancel_btn):
                event.stop()
    


class Pipeline(App):
    CSS_PATH = "style.tcss"


    def __init__(self):
        super().__init__()
        self.years, self.months, self.vendors = get_years_months_vendors()
        self.selected_years = []
        self.selected_months = []


    def compose(self):
        with Middle():
            with Center():
                with Vertical(id="dialog"):
                    yield Label("Data Pipeline Settings", id="title")

                    with Horizontal(classes="optbox-row"):
                        yield Label("Download mode:")
                        yield OptionBox(
                            ["None", "Missing Only", "All"],
                            id="dl_mode_selector",
                            classes="focuseable"
                        )
                    
                    with Vertical(id="dl_mode-collapsable"):
                        with Horizontal(classes="optbox_sub1-row"):
                            yield Label("Vendors:")
                            yield OptionBox(
                                ["All", "Custom"],
                                id="dl_selector",
                                classes="focuseable"
                            )

                        with Vertical(id="vendors-collapsable"):
                            for vendor in self.vendors:
                                short_name = vendor.split(sep=' ', maxsplit=1)[0].lower()

                                with Horizontal(classes="optbox_sub2-row"):
                                    yield CheckBox(
                                        state=True,
                                        id=f"{short_name}_checkbox",
                                        classes="focuseable"
                                    )
                                    yield Label(vendor)

                        with Horizontal(classes="optbox_sub1-row"):
                            yield Label("Dates:")
                            yield OptionBox(
                                ["All", "Custom"],
                                id="date_selector",
                                classes="focuseable"
                            )

                        with Vertical(id="date-collapsable"):
                            with Horizontal(classes="optbox_sub2-row middle-button"):
                                yield Button(
                                    "Select Dates",
                                    action=self.open_date_picker,
                                    id="date-popup-button",
                                    classes="focuseable"
                                )

                        with Horizontal(classes="optbox_sub1-row"):
                            yield CheckBox(
                                state=False,
                                id="tf_selector",
                                classes="focuseable"
                            )
                            yield Label("Apply column transformations")

                    with Horizontal(classes="optbox-row"):
                        yield Label("Merge type:")
                        yield OptionBox(
                            ["None", "By vendor", "By month", "By year", "Single file"],
                            id="merge_selector",
                            classes="focuseable"
                        )

                    with Vertical(id="merge-collapsable"):
                        with Horizontal(classes="optbox_sub1-row"):
                            yield CheckBox(
                                state=False,
                                id="del-files-merge-checkbox",
                                classes="focuseable"
                            )
                            yield Label("Delete original files after merge")

                    with Horizontal(classes="optbox-row"):
                        yield CheckBox(
                            state=False,
                            id="outlier_selector",
                            classes="focuseable"
                        )
                        yield Label("Remove outliers")
                    
                    with Vertical(classes="down-right"):
                        yield Button("Download", action=self.pipeline, classes="focuseable")
                        yield Button("Exit", action=self.exit, classes="focuseable")


    def open_date_picker(self):
        def handle_dates(data):
            if data:
                self.selected_years = data["years"]
                self.selected_months = data["months"]

        self.push_screen(DatePickerModal(self.years, self.months), handle_dates)


    def on_key(self, event: events.Key) -> None:
        if isinstance(self.focused, SelectionList):
            return

        if event.key == "up":
            self.screen.focus_previous()
        elif event.key == "down":
            self.screen.focus_next()

    
    def on_option_box_changed(self, message: OptionBox.Changed) -> None:
        if message.sender.id == "dl_mode_selector":
            self.query_one("#dl_mode-collapsable").display = (message.value != "None")

        elif message.sender.id == "dl_selector":
            self.query_one("#vendors-collapsable").display = (message.value == "Custom")

        elif message.sender.id == "merge_selector":
            self.query_one("#merge-collapsable").display = (message.value != "None")

        elif message.sender.id == "date_selector":
            self.query_one("#date-collapsable").display = (message.value == "Custom")


    @work(exclusive=True, thread=True)
    def pipeline(self):
        from data_extraction.download import (
            get_lazy_frame,
            apply_transformations,
            save_lazy_frame,
            merge_lazy_frames,
            rm_outliers
        )
        import polars as pl
        from itertools import product
        from textual import log
        from time import sleep
        from random import random
        from pathlib import Path

        years = self.selected_years
        months = self.selected_months

        vendor_mode = self.query_one("#dl_selector").value
        del_files_merge = self.query_one("#del-files-merge-checkbox").state
        merge_type = self.query_one("#merge_selector").value
        transf = self.query_one("#tf_selector").state
        dl_mode = self.query_one("#dl_mode_selector").value
        outliers = self.query_one("#outlier_selector").state
        dl_filepaths: list[Path] = []
        
        # Get vendors to download
        vendors = []
        vendor_map = {
            "yellow_checkbox": "yellow",
            "green_checkbox": "green",
            "for-hire_checkbox": "fhv",
            "high_checkbox": "fhvhv"
        }

        if vendor_mode == "All":
            vendors = [val for val in vendor_map.values()]
        else:
            for v_id, name in vendor_map.items():
                widget = self.query_one(f"#{v_id}")
                if widget.state: 
                    vendors.append(name)

        # ------------------------------------- #
        # ----- Beginning of the pipeline ----- #
        # ------------------------------------- #
        self.call_from_thread(setattr, self.screen, "disabled", True)

        if dl_mode != "None" or len(vendors) == 0:
            for group in product(years, months, vendors):

                # Check for duplicate is Missing Only is selected
                file_path = Path(Path.cwd(), "data", str(group[0]), str(group[1]), f"{group[2]}.parquet")
                if dl_mode == "Missing Only" and file_path.exists():
                    continue

                # Try to download the file
                self.notify(f"Downloading data from {group[2]}-{group[1]}-{group[0]}", title="Download in progress")
                lf = get_lazy_frame(*group)

                if isinstance(lf, tuple):
                    if lf[0] == -1:
                        self.notify(f"HTTP Error: {lf[1]} {list(group)}", title="Download error")
                    elif lf[0] == -2:
                        self.notify(f"Invalid Content-type: {lf[1]} {list(group)}", title="Content-type error")
                    continue
                self.notify(f"File downloaded correctly from web {list(group)}!", title="Success")

                # Apply transformations if requested
                if transf:
                    lf = apply_transformations(lf, group[2])
                    self.notify(f"Data transformed correctly {list(group)}!", title="Success")

                # Save data to local file
                dl_filepaths.append(save_lazy_frame(lf, *group))
                self.notify(f"File saved correctly! {list(group)}", title="Success")

                sleep(2 * (1 + random()))

        # Merge local files if requested
        if merge_type != "None":
            self.notify("Merging data...", title="Success")
            aux = merge_lazy_frames(method=merge_type, remove_files=del_files_merge)
            if aux is not None:
                dl_filepaths = aux
            self.notify("Data merged successfully...", title="Success")
        else:
            dl_filepaths = [_ for _ in Path(Path.cwd(), "data").glob("*.parquet")]

        if outliers:
            # self.notify(f"{dl_filepaths}", title="Success")
            self.notify("Removing outliers. Please wait...", title="Success")
            for f in dl_filepaths:
                rm_outliers(f)
            self.notify("Outliers removed successfully...", title="Success")

        self.call_from_thread(setattr, self.screen, "disabled", False)



if __name__ == '__main__':
    Pipeline().run()

