from pipeline.widgets import *
from pipeline.selection_tree import TreeSelectionModal
from textual import events, work
from textual.app import App, ComposeResult
from textual.containers import Center, Middle, Horizontal, Vertical
from textual.widgets import (
    Footer,
    ContentSwitcher,
    Tab,
    Label
)


class Pipeline(App):
    CSS_PATH = "style.tcss"

    TAB_LIST = ["Download", "Preprocess", "Logs"]

    BINDINGS = [
        *[(str(i+1), f"switch_tab('content-tab-{i+1}', 'nav-tab-{i+1}')", val)
        for i, val in enumerate(TAB_LIST)],
    ]


    def __init__(self):
        from pipeline.pl_utils import get_years_months_vendors, get_parquet_files
        from minio_utils import MinioSparkClient
        from os import getenv

        super().__init__()
        self.dates, self.vendors = get_years_months_vendors()  #type:ignore
        self.selected_dates = None
        self.files = get_parquet_files()
        self.minio_files = None
        self.selected_files = None
        self.selected_minio_files = None

        self._client = MinioSparkClient(
            endpoint="minio.fdi.ucm.es",
            access_key=getenv("MINIO_ACCESS_KEY"),  #type:ignore
            secret_key=getenv("MINIO_SECRET_KEY"),  #type:ignore
            bucket_name="pd2",
            base_dir="cityenjoyer",
            memory = 8,
            heapsize = 4,
            num_part = 100,
            verbose=False
        )


    def add_log(self, message: str, status: str = "INFO"):
        try:
            log_view = self.query_one("#log-view", LogView)
            self.call_from_thread(log_view.add_line, message, status)
        except Exception:
            pass


    def notify_and_log(self, message: str, title: str = "", status: str = "INFO", markup: bool = True):
        self.notify(message, title=title, markup=markup)
        self.add_log(f"{f"({title}):":28} {message}" if title else message, status)


    def _exit_app(self):
        self._client.disconnect()
        self.exit()


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
            TreeSelectionModal(
                self.dates,
                self.selected_dates,  #type:ignore
                title_text="Select Dates"
            ),
            handle_return
        )


    def open_file_picker(self):
        from pipeline.pl_utils import get_parquet_files

        def handle_return(data):
            if data is not None:
                self.selected_files = data

        def handle_minio_return(data):
            if data is not None:
                self.selected_minio_files = data

        value = self.query_one("#file_location_selector").value  #type:ignore
        if value == "Minio" and self.minio_files is None:
            self.minio_files = get_parquet_files(client=self._client)

        self.push_screen(
            TreeSelectionModal(
                data=self.minio_files if value == "Minio" else self.files,  #type:ignore
                selected_data=(
                    self.selected_files
                    if value != "Minio"
                    else self.selected_minio_files
                ),
                title_text="Select files to prepare for model",
                client=self._client if value == "Minio" else None
            ),
            handle_return if value != "Minio" else handle_minio_return
        )


    def on_key(self, event: events.Key) -> None:
        if event.key == "escape":
            event.stop()
            self._exit_app()
        elif event.key == "up":
            self.screen.focus_previous()
        elif event.key == "down":
            self.screen.focus_next()


    def on_option_box_changed(self, message: OptionBox.Changed) -> None:
        if message.sender.id == "dl_selector":
            self.query_one("#vendors-collapsable").display = (message.value == "Custom")

        elif message.sender.id == "date_selector":
            self.query_one("#date-collapsable").display = (message.value == "Custom")


    def on_check_box_changed(self, message: CheckBox.Changed) -> None:
        return


    @work(exclusive=True, thread=True)
    def run_dl_pipeline(self):
        from itertools import product
        from os import environ
        from time import sleep
        from random import random
        from pathlib import Path
        from data_extraction.download import (
            get_lazy_frame,
            save_lazy_frame
        )
        from data_preprocessing.preprocess import transform_columns

        dl_mode = self.query_one("#dl_mode_selector").value  #type:ignore
        dl_location = self.query_one("#dl_location_selector").value  #type:ignore
        transf = self.query_one("#tf_selector").value  #type:ignore
        vendor_mode = self.query_one("#dl_selector").value  #type:ignore
        date_mode = self.query_one("#date_selector").value  #type:ignore

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
                if widget.value:   #type:ignore
                    vendors.append(name)

        # ------------------------------------- #
        # ----- Beginning of the pipeline ----- #
        # ------------------------------------- #
        self.call_from_thread(lambda: [setattr(w, "disabled", True) for w in self.query("#dialog, #dialog2")])

        if dl_mode != "None" or len(vendors) == 0:
            for group in product(dates, vendors):
                date = group[0].split("-")

                # Check for duplicate is Missing Only is selected
                file_path = Path(environ["PD2_DATA_DIR"], date[0], date[1].lstrip('0'), f"{group[1]}.parquet")
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
                    lf = transform_columns(lf, group[1], group[0])
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
                save_lazy_frame(lf, date[0], date[1], group[1], self._client if dl_location != "Local" else None)
                self.notify_and_log(
                    message=f"File saved correctly! {list(group)}",
                    title="File save successful",
                    status="SUCCESS"
                )

                sleep(2 * (1 + random()))

        self.call_from_thread(lambda: [setattr(w, "disabled", False) for w in self.query("#dialog, #dialog2")])


    @work(exclusive=True, thread=True)
    def run_prep_pipeline(self):
        from data_preprocessing.preprocess import (
            remove_outliers_local,
            remove_outliers_minio,
            merge_files_local,
            merge_files_minio,
            prepare_data_local,
            prepare_data_minio
        )


        file_location = self.query_one("#file_location_selector").value  #type:ignore
        del_outliers = self.query_one("#outlier-checkbox").value  #type:ignore
        merge = self.query_one("#merge_checkbox").value  #type:ignore
        prep_model = self.query_one("#prep_model_checkbox").value  #type:ignore

        files = self.selected_files if file_location == "Local" else self.selected_minio_files
        if files is None or len(files) <= 0:
            self.notify(message="No files were selected")
            return

        if prep_model and len(files) > 1 and not merge:
            self.notify("Multiple files selected for aggregation, \
                but no merge requested. Please schedule merge or select one file only.")
            return

        if file_location == "Minio" and not self._client.is_connected():
            self._client.connect()
            self.notify_and_log(
                message="Client established connection with MinIO correctly",
                title="Connection successful",
                status="SUCCESS"
            )

        # ------------------------------------- #
        # ----- Beginning of the pipeline ----- #
        # ------------------------------------- #
        self.call_from_thread(lambda: [setattr(w, "disabled", True) for w in self.query("#dialog, #dialog2")])

        res_files = set()
        single_file = None

        if del_outliers:
            self.notify_and_log(
                message="Please wait...",
                title="Removing outliers"
            )

            try:
                if file_location == "Local":
                    aux = remove_outliers_local(filepaths=files)
                else:
                    aux = remove_outliers_minio(filepaths=files, client=self._client)
                
                if aux is not None and len(aux) > 0:
                    res_files = aux

                self.notify_and_log(
                    message="Outliers removed successfully",
                    title="Removal successful",
                    status="SUCCESS"
                )

            except Exception as outlier_exc:
                self.notify_and_log(
                    message=f"Error while removing outliers: {str(outlier_exc)}",
                    title="Outlier Removal error",
                    status="ERROR",
                    markup=False
                )

        if merge:
            self.notify_and_log(
                message="Please wait...",
                title="Merging data"
            )

            try:
                aux = res_files if len(res_files) > 0 else files

                if file_location == "Local":
                    single_file = merge_files_local(aux)
                else:
                    single_file = merge_files_minio(aux, self._client)
                
                self.notify_and_log(
                    message="Data merged successfully",
                    title="Merge successful",
                    status="SUCCESS"
                )

            except Exception as merge_exc:
                self.notify_and_log(
                    message=f"Error while merging data: {str(merge_exc)}",
                    title="Merge error",
                    status="ERROR"
                )

        if prep_model:
            self.notify_and_log(
                message="Please wait...",
                title="Preparing data for model"
            )

            try:
                aux = single_file if single_file is not None else list(files)[0]

                if file_location == "Local":
                    prepare_data_local(aux)
                else:
                    prepare_data_minio(aux, self._client)
                
                self.notify_and_log(
                    message="Data prepared successfully",
                    title="Aggregation successful",
                    status="SUCCESS"
                )

            except Exception as agg_exc:
                self.notify_and_log(
                    message=f"Error while aggregating data: {str(agg_exc)}",
                    title="Aggregation error",
                    status="ERROR"
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
                                yield Label("Save location:")
                                yield OptionBox(
                                    ["Local", "Minio"],
                                    id="dl_location_selector",
                                    classes="focuseable"
                                )

                            with Horizontal(classes="optbox-row"):
                                yield Label("Apply column transformations")
                                yield CheckBox(
                                    is_selected=True,
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
                                yield Button("Exit", action=self._exit_app, classes="focuseable")

            # -------------------------- #
            # ----- Preprocess Tab ----- #
            # -------------------------- #                  
            with Vertical(id="content-tab-2"):
                with Middle():
                    with Center():
                        with Vertical(id="dialog2"):
                            yield Label("Preprocess Settings", id="title2")

                            with Horizontal(classes="optbox-row"):
                                yield Label("Remove outliers")
                                yield CheckBox(
                                    is_selected=False,
                                    id="outlier-checkbox",
                                    classes="focuseable"
                                )
                            
                            with Horizontal(classes="optbox-row"):
                                yield Label("Merge into single file:")
                                yield CheckBox(
                                    is_selected=False,
                                    id="merge_checkbox",
                                    classes="focuseable"
                                )

                            with Horizontal(classes="optbox-row"):
                                yield Label("Prepare data for model:")
                                yield CheckBox(
                                    is_selected=False,
                                    id="prep_model_checkbox",
                                    classes="focuseable"
                                )

                            with Horizontal(classes="optbox-row"):
                                yield Label("File location:")
                                yield OptionBox(
                                    ["Local", "Minio"],
                                    id="file_location_selector",
                                    classes="focuseable"
                                )

                            with Horizontal(classes="optbox-row middle-button"):
                                yield Button(
                                    "Select Files",
                                    action=self.open_file_picker,
                                    id="file-selection-popup-button",
                                    classes="focuseable"
                                )

                            with Vertical(classes="down-right"):
                                yield Button("Start", action=self.run_prep_pipeline, classes="focuseable")
                                yield Button("Exit", action=self._exit_app, classes="focuseable")

            # -------------------- #
            # ----- Logs Tab ----- #
            # -------------------- #                  
            with Vertical(id="content-tab-3"):
                yield Label("Pipeline Logs", classes="title")
                yield LogView(id="log-view")

        yield Footer()
