from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
import uvicorn

from .api import router

base_path = Path(__file__).resolve().parents[1]

# Create app
app = FastAPI()

# Mount the static content
app.mount("/static", StaticFiles(directory=base_path/"static"), name="static")

# Configure templates
templates = Jinja2Templates(directory=base_path/"templates")

# Add api routes
app.include_router(router, prefix="/api")

# Root page
@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)

