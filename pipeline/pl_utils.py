from minio_utils import MinioSparkClient


def get_years_months_vendors() -> tuple[dict[str, dict[str, str]], list[str]] | None:
    from bs4 import BeautifulSoup
    import requests as rq

    response = rq.get("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    if response.status_code // 100 != 2:
        print(f"[Error] Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    dates = {
        tag_div.get("id")[3:]: {  #type:ignore
            tag_strong.text: f"{tag_div.get("id")[3:]}-{i+1:0>2}"  #type:ignore
            for i, tag_strong in enumerate(tag_div.find_all("strong"))
        }
        for tag_div
        in soup.find_all("div", {"class": "faq-answers"})
    }

    vendors = list()
    for td in soup.find_all("td"):
        # Get available vendors
        for link in td.find_all("a"):
            v = link.get("title")[:-13]  #type:ignore
            if v not in vendors:
                vendors.append(v)

    return (dates, vendors)  #type:ignore


def get_parquet_files(client: MinioSparkClient | None = None) -> dict[str, dict]:
    from pathlib import Path

    def add_file(data: dict, parts: tuple, final_value):
        if len(parts) == 0:
            return

        if len(parts) == 1:
            data[parts[0]] = final_value
            return

        if parts[0] not in data:
            data[parts[0]] = {}

        add_file(data[parts[0]], parts[1:], final_value)


    res = {}

    if client is None:
        data_path: Path = Path.cwd() / "data"
        if data_path.exists():
            for file_path in data_path.rglob("*.parquet"):
                if file_path.is_file():
                    add_file(res, file_path.relative_to(data_path).parts, str(file_path))

    else:
        objects = client.list_objects(path="", recursive=True)
        
        for obj in objects:
            if (obj.object_name.endswith(".parquet")  #type:ignore
            and not obj.object_name.endswith("snappy.parquet")):  #type:ignore
                add_file(res, Path(obj.object_name).parts, obj.object_name.replace("cityenjoyer/", ""))  #type:ignore

    return res


def remove_files(files, data_path):
    for f in files:
        f.unlink()

    for p in sorted(data_path.glob("**/*"), reverse=True):  # Remove child directories before parents
        if p.is_dir() and not any(p.iterdir()):
            p.rmdir()
