from pipeline.widgets import *
from pipeline.date_picker import DatePickerModal
from textual import events, work
from textual.app import App, ComposeResult
from textual.screen import ModalScreen
from textual.containers import Center, Middle, Horizontal, Vertical
from textual.widgets import (
    Header,
    Footer,
    ContentSwitcher,
    Tab,
    Label,
    SelectionList
)


def get_years_months_vendors() -> tuple[dict[str, dict[str, str]], list[str]] | None:
    from bs4 import BeautifulSoup
    import requests as rq

    response = rq.get("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    if response.status_code // 100 != 2:
        print(f"[Error] Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    dates = {
        tag_div.get("id")[3:]: {
            tag_strong.text: f"{tag_div.get("id")[3:]}-{i+1:0>2}"
            for i, tag_strong in enumerate(tag_div.find_all("strong"))
        }
        for tag_div in soup.find_all("div", {"class": "faq-answers"})
    }

    vendors = list()
    for td in soup.find_all("td"):
        # Get available vendors
        for link in td.find_all("a"):
            v = link.get("title")[:-13]
            if v not in vendors:
                vendors.append(v)

    return (dates, vendors)



class Pipeline(App):
    CSS_PATH = "style.tcss"

    TAB_LIST = ["Download", "Preprocess", "Logs"]

    BINDINGS = [
        *[(str(i+1), f"switch_tab('content-tab-{i+1}', 'nav-tab-{i+1}')", val)
        for i, val in enumerate(TAB_LIST)],
    ]


    def __init__(self):
        super().__init__()
        self.dates, self.vendors = get_years_months_vendors()
        self.selected_dates = None


    def add_log(self, message: str, status: str = "INFO"):
        try:
            log_view = self.query_one("#log-view", LogView)
            self.call_from_thread(log_view.add_line, message, status)
        except Exception:
            pass


    def notify_and_log(self, message: str, title: str = "", status: str = "INFO"):
        self.notify(message, title=title)
        self.add_log(f"{f"({title}):":28} {message}" if title else message, status)


    def action_switch_tab(self, content_id: str, nav_id: str) -> None:
        self.query_one(ContentSwitcher).current = content_id
        self.query_one(Tabs).active = nav_id

    
    def _focus_first(self, content_id: str):
        focusables = list(self.query_one(f"#{content_id}").query(".focuseable"))

        if focusables:
            focusables[0].focus()


    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        if event.tab and event.tab.id:
            content_id = event.tab.id.replace("nav-", "content-")
            self.query_one(ContentSwitcher).current = content_id

            # Focus first focusable widget inside the tab
            self.call_after_refresh(self._focus_first, content_id)


    def open_date_picker(self):
        def handle_return(data):
            if data is not None:
                self.selected_dates = data

        self.push_screen(
            DatePickerModal(
                self.dates,
                self.selected_dates
            ),
            handle_return
        )


    def on_key(self, event: events.Key) -> None:
        if isinstance(self.focused, SelectionList):
            return

        if event.key == "up":
            self.screen.focus_previous()
        elif event.key == "down":
            self.screen.focus_next()


    def on_option_box_changed(self, message: OptionBox.Changed) -> None:
        if message.sender.id == "dl_selector":
            self.query_one("#vendors-collapsable").display = (message.value == "Custom")

        elif message.sender.id == "date_selector":
            self.query_one("#date-collapsable").display = (message.value == "Custom")

        elif message.sender.id == "merge_selector":
            self.query_one("#merge-collapsable").display = (message.value != "None")


    @work(exclusive=True, thread=True)
    def run_dl_pipeline(self):
        import polars as pl
        from itertools import product
        from time import sleep
        from random import random
        from pathlib import Path
        from data_extraction.download import (
            get_lazy_frame,
            save_lazy_frame
        )
        from data_preprocessing.preprocess import transform_columns

        dl_mode = self.query_one("#dl_mode_selector").value
        transf = self.query_one("#tf_selector").is_selected
        vendor_mode = self.query_one("#dl_selector").value
        date_mode = self.query_one("#date_selector").value

        if date_mode == "All":
            dates = []
            for month in self.dates.values():
                for val in month.values():
                    dates.append(val)
            dates = sorted(dates)
        else:
            if self.selected_dates == None or len(self.selected_dates) == 0:
                self.notify("No dates were selected.")
                return

            dates = sorted(list(self.selected_dates))

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
                if widget.is_selected: 
                    vendors.append(name)

        # ------------------------------------- #
        # ----- Beginning of the pipeline ----- #
        # ------------------------------------- #
        self.call_from_thread(lambda: [setattr(w, "disabled", True) for w in self.query("#dialog, #dialog2")])

        if dl_mode != "None" or len(vendors) == 0:
            for group in product(dates, vendors):
                date = group[0].split("-")

                # Check for duplicate is Missing Only is selected
                file_path = Path(Path.cwd(), "data", *date, f"{group[1]}.parquet")
                if dl_mode == "Missing Only" and file_path.exists():
                    continue

                # Try to download the file
                self.notify_and_log(
                    message="Please wait...",
                    title="Downloading data"
                )
                lf, url = get_lazy_frame(*group)

                if isinstance(lf, int):
                    if lf == -1:
                        self.notify_and_log(
                            message=f"HTTP Error: {url} {list(group)}",
                            title="Download error",
                            status="ERROR"
                        )
                    elif lf == -2:
                        self.notify_and_log(
                            message=f"Invalid Content-type: {url} {list(group)}",
                            title="Download error",
                            status="ERROR"
                        )
                    continue
                self.notify_and_log(
                    message=f"File downloaded correctly from '{url}'",
                    title="Download successful",
                    status="SUCCESS"
                )

                # Apply transformations if requested
                if transf:
                    self.notify_and_log(
                        message="Please wait...",
                        title="Transforming columns"
                    )
                    lf = transform_columns(lf, group[1])
                    self.notify_and_log(
                        message=f"Data transformed correctly {list(group)}!",
                        title="Transformation successful",
                        status="SUCCESS"
                    )

                # Save data to local file
                self.notify_and_log(
                    message="Please wait...",
                    title="Saving data to file"
                )
                save_lazy_frame(lf, *date, group[1])
                self.notify_and_log(
                    message=f"File saved correctly! {list(group)}",
                    title="File save successful",
                    status="SUCCESS"
                )

                sleep(2 * (1 + random()))

        self.call_from_thread(lambda: [setattr(w, "disabled", False) for w in self.query("#dialog, #dialog2")])


    @work(exclusive=True, thread=True)
    def run_prep_pipeline(self):
        from data_preprocessing.preprocess import merge_lazy_frames, remove_outliers
        from pathlib import Path


        merge_type = self.query_one("#merge_selector").value
        del_files_merge = self.query_one("#del-files-merge-checkbox").is_selected
        outliers = self.query_one("#outlier_selector").is_selected
        merged_files: list[Path] = []

        # ------------------------------------- #
        # ----- Beginning of the pipeline ----- #
        # ------------------------------------- #
        self.call_from_thread(lambda: [setattr(w, "disabled", True) for w in self.query("#dialog, #dialog2")])

        merge_path = Path(Path.cwd(), "data", "merged")

        if merge_type != "None":
            self.notify_and_log(
                message="Please wait...",
                title="Merging data"
            )

            aux = merge_lazy_frames(method=merge_type, remove_files=del_files_merge)
            if aux is not None:
                merged_files = aux

            self.notify_and_log(
                message="Data merged successfully",
                title="Merge successful",
                status="SUCCESS"
            )
        elif merge_path.exists():
            merged_files = [_ for _ in merge_path.glob("*.parquet")]

        if outliers:
            self.notify_and_log(
                message="Please wait...",
                title="Removing outliers"
            )

            outlier_ok = True

            for f in merged_files:
                try:
                    remove_outliers(f)
                except Exception as outlier_exc:
                    self.notify_and_log(
                        message=f"Error while removing outliers from {f}",
                        title="Removal error",
                        status="ERROR"
                    )
                    self.notify_and_log(
                        message=str(outlier_exc)
                    )
                    outlier_ok = False
            
            if outlier_ok:
                self.notify_and_log(
                    message="Outliers removed successfully",
                    title="Removal successful",
                    status="SUCCESS"
                )

        self.call_from_thread(lambda: [setattr(w, "disabled", False) for w in self.query("#dialog, #dialog2")])


    def compose(self) -> ComposeResult:
        yield Tabs(*[Tab(val, id=f"nav-tab-{i+1}") for i, val in enumerate(self.TAB_LIST)])
        
        with ContentSwitcher(initial="tab-1"):

            # ------------------------ #
            # ----- Downlaod Tab ----- #
            # ------------------------ #
            with Vertical(id="content-tab-1"):
                with Middle():
                    with Center():
                        with Vertical(id="dialog"):
                            yield Label("Download Settings", id="title")
                            
                            with Horizontal(classes="optbox-row"):
                                yield Label("Download mode:")
                                yield OptionBox(
                                    ["All", "Missing Only"],
                                    id="dl_mode_selector",
                                    classes="focuseable"
                                )

                            with Horizontal(classes="optbox-row"):
                                yield Label("Apply column transformations")
                                yield CheckBox(
                                    is_selected=True,
                                    id="tf_selector",
                                    classes="focuseable"
                                )
                                # yield Label("Apply column transformations")

                            with Horizontal(classes="optbox-row"):
                                yield Label("Vendors:")
                                yield OptionBox(
                                    ["All", "Custom"],
                                    id="dl_selector",
                                    classes="focuseable"
                                )

                            with Vertical(id="vendors-collapsable"):
                                for vendor in self.vendors:
                                    short_name = vendor.split(sep=' ', maxsplit=1)[0].lower()

                                    with Horizontal(classes="optbox_sub1-row"):
                                        yield CheckBox(
                                            is_selected=True,
                                            id=f"{short_name}_checkbox",
                                            classes="focuseable"
                                        )
                                        yield Label(vendor)

                            with Horizontal(classes="optbox-row"):
                                yield Label("Dates to download:")
                                yield OptionBox(
                                    ["All", "Custom"],
                                    id="date_selector",
                                    classes="focuseable"
                                )

                            with Vertical(id="date-collapsable"):
                                with Horizontal(classes="optbox_sub1-row middle-button"):
                                    yield Button(
                                        "Select Dates",
                                        action=self.open_date_picker,
                                        id="date-popup-button",
                                        classes="focuseable"
                                    )

                            with Vertical(classes="down-right"):
                                yield Button("Download", action=self.run_dl_pipeline, classes="focuseable")
                                yield Button("Exit", action=self.exit, classes="focuseable")

            # -------------------------- #
            # ----- Preprocess Tab ----- #
            # -------------------------- #                  
            with Vertical(id="content-tab-2"):
                with Middle():
                    with Center():
                        with Vertical(id="dialog2"):
                            yield Label("Preprocess Settings", id="title2")
                            
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
                                        is_selected=False,
                                        id="del-files-merge-checkbox",
                                        classes="focuseable"
                                    )
                                    yield Label("Delete original files after merge")

                            with Horizontal(classes="optbox-row"):
                                yield CheckBox(
                                    is_selected=False,
                                    id="outlier_selector",
                                    classes="focuseable"
                                )
                                yield Label("Remove outliers")
                            
                            with Vertical(classes="down-right"):
                                yield Button("Start", action=self.run_prep_pipeline, classes="focuseable")
                                yield Button("Exit", action=self.exit, classes="focuseable")

            # -------------------- #
            # ----- Logs Tab ----- #
            # -------------------- #                  
            with Vertical(id="content-tab-3"):
                yield Label("Pipeline Logs", classes="title")
                yield LogView(id="log-view")

        yield Footer()
