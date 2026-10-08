#!/usr/bin/env python3
import os
import re
import time
import uuid
import shutil
import asyncio
import logging
import subprocess
from pathlib import Path

import aiohttp
import aiofiles
import aioboto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"

WORK_DIR = "/tmp/hls_jobs"
os.makedirs(WORK_DIR, exist_ok=True)

MAX_INPUT_SIZE = 2 * 1024 * 1024 * 1024
SEG_DURATION = 1
DOWNLOAD_CHUNK = 1024 * 1024
QUEUE_WORKERS = 1

QUALITIES = [
    ("240p", 426, 240, "300k", "64k"),
    ("360p", 640, 360, "700k", "96k"),
    ("480p", 854, 480, "1200k", "128k"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("bot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("aiobotocore").setLevel(logging.WARNING)

app = Client(
    os.path.join(WORK_DIR, "session"),
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=4,
)

_s3_config = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=20,
    connect_timeout=30,
    read_timeout=300,
)
_s3_session = aioboto3.Session()


def content_type_for(name):
    if name.endswith(".m3u8"):
        return "application/vnd.apple.mpegurl"
    if name.endswith(".m4s"):
        return "video/iso.segment"
    if name.endswith(".mp4"):
        return "video/mp4"
    return "application/octet-stream"


async def s3_put(local_path, remote_key):
    async with _s3_session.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public", aws_secret_access_key="public",
        region_name=S3_REGION, config=_s3_config,
    ) as s3:
        with open(local_path, "rb") as f:
            await s3.put_object(
                Bucket=S3_BUCKET, Key=remote_key, Body=f,
                ContentType=content_type_for(local_path),
            )


def fmt_size(b):
    if b < 1024:
        return f"{b} B"
    if b < 1048576:
        return f"{b/1024:.1f} KB"
    if b < 1073741824:
        return f"{b/1048576:.1f} MB"
    return f"{b/1073741824:.2f} GB"


async def detect_fps(path):
    def _run():
        return subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=r_frame_rate", "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
    raw = await asyncio.to_thread(_run)
    if not raw or "/" not in raw:
        return 30
    try:
        n, d = raw.split("/")
        return round(int(n) / int(d))
    except Exception:
        return 30


class MsgEditor:
    def __init__(self, chat_id, msg_id):
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.last = 0.0
        self.last_text = ""

    async def edit(self, text, force=False):
        now = time.time()
        if not force and now - self.last < 2.0:
            return
        if text == self.last_text and not force:
            return
        try:
            await app.edit_message_text(self.chat_id, self.msg_id, text)
            self.last = now
            self.last_text = text
        except Exception:
            pass

    async def final(self, text, markup=None):
        try:
            await app.edit_message_text(
                self.chat_id, self.msg_id, text, reply_markup=markup,
            )
        except Exception:
            pass


class Job:
    def __init__(self, uid, chat_id, msg_id, url):
        self.job_id = uuid.uuid4().hex[:12]
        self.user_id = uid
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.url = url
        self.task = None
        self.cancelled = False


queue = asyncio.Queue()
user_pending = {}


async def enqueue(job):
    if job.user_id in user_pending:
        raise ValueError("already")
    user_pending[job.user_id] = job
    await queue.put(job)
    return queue.qsize()


async def cancel_user(uid):
    job = user_pending.get(uid)
    if job is None:
        return "none"
    job.cancelled = True
    if job.task and not job.task.done():
        job.task.cancel()
        return "active"
    user_pending.pop(uid, None)
    return "queued"


async def process(job):
    work = Path(WORK_DIR) / job.job_id
    work.mkdir(parents=True, exist_ok=True)
    input_file = work / "input.mp4"
    out_dir = work / "out"
    editor = MsgEditor(job.chat_id, job.msg_id)

    try:
        await editor.edit("Descargando video...", force=True)

        timeout = aiohttp.ClientTimeout(total=7200, connect=30, sock_read=300)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(job.url, headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.status >= 400:
                    raise RuntimeError(f"HTTP {r.status}")
                total = int(r.headers.get("Content-Length", 0))
                if total and total > MAX_INPUT_SIZE:
                    raise RuntimeError(f"Video muy grande: {fmt_size(total)}")
                got = 0
                async with aiofiles.open(input_file, "wb") as f:
                    async for chunk in r.content.iter_chunked(DOWNLOAD_CHUNK):
                        await f.write(chunk)
                        got += len(chunk)
                        if got > MAX_INPUT_SIZE:
                            raise RuntimeError("Excede limite")
                        if total and got % (5 * 1024 * 1024) < DOWNLOAD_CHUNK:
                            pct = int(got / total * 100)
                            await editor.edit(f"Descargando... {pct}%")

        if job.cancelled:
            raise asyncio.CancelledError()

        fps = await detect_fps(str(input_file))
        gop = fps * SEG_DURATION

        await editor.edit("Convirtiendo a HLS...", force=True)

        out_dir.mkdir(exist_ok=True)
        for name, *_ in QUALITIES:
            (out_dir / name).mkdir(exist_ok=True)

        n = len(QUALITIES)
        split = "".join(f"[v{i}]" for i in range(n))
        scale = ";".join(
            f"[v{i}]scale=w={w}:h={h}:force_original_aspect_ratio=decrease,"
            f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2[v{i}out]"
            for i, (_, w, h, _, _) in enumerate(QUALITIES)
        )
        fc = f"[0:v]split={n}{split};{scale}"

        cmd = ["ffmpeg", "-y", "-i", str(input_file), "-filter_complex", fc]
        for i, (_, _, _, vb, _) in enumerate(QUALITIES):
            cmd += [
                "-map", f"[v{i}out]",
                f"-c:v:{i}", "libx264",
                f"-b:v:{i}", vb,
                f"-maxrate:v:{i}", vb,
                f"-bufsize:v:{i}", f"{int(vb[:-1])*2}k",
                "-preset", "veryfast",
                "-force_key_frames", f"expr:gte(t,n_forced*{SEG_DURATION})",
                "-g", str(gop), "-keyint_min", str(gop), "-sc_threshold", "0",
            ]
        for i, (_, _, _, _, ab) in enumerate(QUALITIES):
            cmd += ["-map", "a:0", f"-c:a:{i}", "aac", f"-b:a:{i}", ab, "-ac", "2"]

        var_map = " ".join(
            f"v:{i},a:{i},name:{name}" for i, (name, *_ ) in enumerate(QUALITIES)
        )

        cmd += [
            "-f", "hls",
            "-hls_time", str(SEG_DURATION),
            "-hls_playlist_type", "vod",
            "-hls_flags", "independent_segments",
            "-hls_segment_type", "fmp4",
            "-hls_fmp4_init_filename", "init.mp4",
            "-hls_segment_filename", f"{out_dir}/%v/segment_%05d.m4s",
            "-master_pl_name", "master.m3u8",
            "-var_stream_map", var_map,
            f"{out_dir}/%v/playlist.m3u8",
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        async for _ in proc.stderr:
            pass
        await proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"FFmpeg codigo {proc.returncode}")

        if job.cancelled:
            raise asyncio.CancelledError()

        await editor.edit("Subiendo a Todus S3...", force=True)

        remote_prefix = f"hls/{job.job_id}"
        files = sorted(f for f in out_dir.rglob("*") if f.is_file())
        total_files = len(files)
        uploaded = 0
        for f in files:
            if job.cancelled:
                raise asyncio.CancelledError()
            rel = f.relative_to(out_dir).as_posix()
            try:
                await s3_put(str(f), f"{remote_prefix}/{rel}")
            except Exception as e:
                log.warning(f"Error subiendo {rel}: {e}")
            uploaded += 1
            if uploaded % 20 == 0:
                await editor.edit(f"Subiendo... {uploaded}/{total_files}")

        master_url = f"{S3_ENDPOINT}/{S3_BUCKET}/{remote_prefix}/master.m3u8"
        total_bytes = sum(f.stat().st_size for f in files)

        await editor.final(
            f"HLS listo\n\n"
            f"Calidades: 240p / 360p / 480p\n"
            f"{total_files} archivos, {fmt_size(total_bytes)}\n\n"
            f"Master:\n{master_url}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Abrir master.m3u8", url=master_url),
            ]]),
        )

    except asyncio.CancelledError:
        await editor.final("Cancelado.")
        raise
    except Exception as e:
        log.exception("Job fallo")
        await editor.final(f"Error: {str(e)[:300]}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


async def worker(wid):
    log.info(f"Worker {wid} listo")
    while True:
        job = await queue.get()
        try:
            if job.cancelled:
                continue
            job.task = asyncio.create_task(process(job))
            try:
                await job.task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.exception(f"Job {job.job_id}: {e}")
        finally:
            user_pending.pop(job.user_id, None)
            queue.task_done()


URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)
SKIP = ["start", "cancel", "status"]


@app.on_message(filters.command("start"))
async def cmd_start(client, message):
    await message.reply_text(
        "HLS Converter Bot\n\n"
        "Enviame un enlace directo a un video y lo convierto a HLS:\n\n"
        "240p / 360p / 480p, segmentos .m4s de 1s\n"
        "Subido a Todus S3\n\n"
        "1 trabajo a la vez por usuario.\n"
        "/cancel para cancelar"
    )


@app.on_message(filters.command("cancel"))
async def cmd_cancel(client, message):
    r = await cancel_user(message.from_user.id)
    if r == "none":
        await message.reply_text("Sin trabajos.")
    elif r == "active":
        await message.reply_text("Cancelando...")
    else:
        await message.reply_text("Removido de la cola.")


@app.on_message(filters.command("status"))
async def cmd_status(client, message):
    uid = message.from_user.id
    if uid in user_pending:
        await message.reply_text(f"Job activo. Cola: {queue.qsize()}")
    else:
        await message.reply_text(f"Sin trabajos. Cola: {queue.qsize()}")


@app.on_message(filters.text & ~filters.command(SKIP))
async def handle_url(client, message):
    uid = message.from_user.id
    m = URL_RE.search(message.text.strip())
    if not m:
        await message.reply_text("Enviame un enlace a un video.")
        return
    if uid in user_pending:
        await message.reply_text("Ya tienes un trabajo. /status o /cancel")
        return
    status = await message.reply_text("Aceptado, en cola...")
    job = Job(uid, message.chat.id, status.id, m.group(0))
    try:
        pos = await enqueue(job)
    except ValueError:
        await status.edit_text("Ya tienes un trabajo.")
        return
    if pos == 1:
        await status.edit_text("Procesando...")
    else:
        await status.edit_text(f"En cola - posicion #{pos}")


async def main():
    for i in range(QUEUE_WORKERS):
        asyncio.create_task(worker(i))
    await app.start()
    log.info("Bot listo")
    try:
        await asyncio.Event().wait()
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
