import os
import shutil

from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from fastapi_tusd import TusRouter

from src.commons import settings

if not os.path.exists(settings.DATA_TMP_BASE_TUS_FILES_DIR):
    os.makedirs(settings.DATA_TMP_BASE_TUS_FILES_DIR)
upload_files = TusRouter(store_dir=settings.DATA_TMP_BASE_TUS_FILES_DIR, location=f'{settings.TUS_BASE_URL}/files', tags=["Files"])
router = APIRouter()

router.include_router(upload_files, prefix="/files")


@router.get("/upload.html", tags=["Files-Utils"])
async def read_uppy():
    return HTMLResponse(html_content)


# fmt: off
html_content = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Uppy</title>
    <link href="https://releases.transloadit.com/uppy/v3.3.1/uppy.min.css" rel="stylesheet">
</head>
<body>
<div id="drag-drop-area"></div>

<script type="module">
    import {Uppy, Dashboard, Tus} from "https://releases.transloadit.com/uppy/v3.3.1/uppy.min.mjs"
    var uppy = new Uppy()
        .use(Dashboard, {
            inline: true,
            target: '#drag-drop-area'
        })
        .use(Tus, {endpoint: '/files'})

    uppy.on('complete', (result) => {
        console.log('Upload complete! We’ve uploaded these files:', result.successful)
    })
</script>
</body>
</html>
"""


# fmt: on


@router.get('/uploaded-files', tags=["Files-Utils"])
def get_uploaded_files():
    return os.listdir("./tus-files")


@router.delete('/all-files', tags=["Files-Utils"])
def delete_files():
    if os.path.exists("./tus-files"):
        shutil.rmtree("./tus-files")
        os.mkdir("./tus-files")
        return "Deleted"
    return {}


@router.get('/disk', tags=["Files-Utils"])
def get_disk_files():
    total, used, free = shutil.disk_usage("/")

    t = ("Total: %d GiB" % (total // (2 ** 30)))
    u = ("Used: %d GiB" % (used // (2 ** 30)))
    f = ("Free: %d GiB" % (free // (2 ** 30)))

    return {"Total:": t, "Used": u, "Free": f}
