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
                            with Horizontal(classes="optbox_sub1-row"):
                                yield Label(vendor)
                                yield OptionBox(["All", "Missing only"], id=f"{vendor[:4]}_selector", classes="focuseable") 
                    
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

        years = [2025, ]
        months = [1, ]
        vendors = ["yellow", "green", "fhvhv"]

        for group in product(years, months, vendors):
            lf = get_lazy_frame(*group)

            if isinstance(lf, tuple):
                if lf[0] == -1:
                    self.notify(f"HTTP Error: {lf[1]} {list(group)}", title="Download error")
                elif lf[0] == -2:
                    self.notify(f"Invalid Content-type: {lf[1]} {list(group)}", title="Content-type error")
                continue

            self.notify(f"File downloaded correctly from web {list(group)}!", title="Success")

            transf_lf = apply_transformations(lf, group[2])
            self.notify(f"Data transformed correctly {list(group)}!", title="Success")

            save_lazy_frame(transf_lf, *group)
            self.notify(f"File saved correctly! {list(group)}", title="Success")

            sleep(2 * (1 + random()))



if __name__ == '__main__':
    Pipeline().run()

