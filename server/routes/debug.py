"""
Temporary debug pages. /debug/viewer exists to confirm the data stream works
end-to-end and to serve as a reference SSE client; delete it once the real
dashboard consumes the stream.
"""
import pathlib

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(prefix="/debug", tags=["debug"])

STATIC_DIR = pathlib.Path(__file__).parent.parent / "static"


@router.get("/viewer", response_class=HTMLResponse)
def stream_viewer() -> str:
    return (STATIC_DIR / "stream_viewer.html").read_text()
