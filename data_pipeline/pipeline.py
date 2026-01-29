from textual.app import App
from textual.containers import Center, Middle, Vertical, Horizontal
from textual.widgets import Label
from textual import events, work
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
    


class Pipeline(App):
    CSS_PATH = "style.tcss"


    def __init__(self):
        super().__init__()
        self.years, self.months, self.vendors = get_years_months_vendors()


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
                        for vendor in self.vendors:
                            short_name = vendor.split(sep=' ', maxsplit=1)[0].lower()

                            with Horizontal(classes="optbox_sub1-row"):
                                yield Label(vendor)
                                yield OptionBox(
                                    ["All", "Missing only", "None"],
                                    id=f"{short_name}_selector",
                                    classes="focuseable"
                                ) 
                    
                    with Vertical(classes="down-right"):
                        yield Button("Download", action=self.download, classes="focuseable")
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


    @work(exclusive=True, thread=True)
    def download(self):
        from data_extraction.download import get_lazy_frame, apply_transformations, save_lazy_frame
        import polars as pl
        from itertools import product
        from textual import log
        from time import sleep
        from random import random
        from pathlib import Path

        self.call_from_thread(setattr, self.screen, "disabled", True)

        years = [2025, ]
        months = [1, 2, 3]
        

        # Get vendors to download
        vendors = []
        vendor_mode = self.query_one("#dl_selector").value
        vendor_map = {
            "yellow_selector": "yellow",
            "green_selector": "green",
            "for-hire_selector": "fhv",
            "high_selector": "fhvhv"
        }

        if vendor_mode == "All":
            vendors = [(val, False) for val in vendor_map.values()]
        else:
            for v_id, name in vendor_map.items():
                widget = self.query_one(f"#{v_id}")
                if widget.value == "None": 
                    continue
                else:
                    vendors.append((name, widget.value == "Missing only"))


        # ------------------------------------- #
        # ----- Beginning of the pipeline ----- #
        # ------------------------------------- #
        for group in product(years, months, vendors):
            # Check if file already exists
            file_path = Path(Path.cwd(), "data", str(group[0]), str(group[1]), f"{group[2][0]}.parquet")
            if group[2][1] and file_path.exists():
                self.notify(f"File {file_path} already exists")
                continue

            info = [group[0], group[1], group[2][0]]
            self.notify(f"Downloading data from {info[2]}-{info[1]}-{info[0]}", title="Download in progress")

            lf = get_lazy_frame(*info)

            if isinstance(lf, tuple):
                if lf[0] == -1:
                    self.notify(f"HTTP Error: {lf[1]} {list(info)}", title="Download error")
                elif lf[0] == -2:
                    self.notify(f"Invalid Content-type: {lf[1]} {list(info)}", title="Content-type error")
                continue

            self.notify(f"File downloaded correctly from web {list(info)}!", title="Success")

            transf_lf = apply_transformations(lf, info[2])
            self.notify(f"Data transformed correctly {list(info)}!", title="Success")

            save_lazy_frame(transf_lf, *info)
            self.notify(f"File saved correctly! {list(info)}", title="Success")

            sleep(2 * (1 + random()))

        self.call_from_thread(setattr, self.screen, "disabled", False)



if __name__ == '__main__':
    Pipeline().run()

