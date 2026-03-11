"""
backup/server.py — Flask backup node server.

Run on an old PC / Raspberry Pi:
    python server.py

Endpoints:
    GET  /health
    POST /store              upload a part file
    POST /store_index        upload index.json
    GET  /list               list logical file names (from index)
    GET  /list_parts         list all raw files in storage
    GET  /get_part/<name>    download a part file
    GET  /get_index          download index.json
    DELETE /delete/<name>    delete a logical file + its parts from disk
    GET  /info               storage stats
"""

from __future__ import annotations

import json
import os

from flask import Flask, jsonify, request, send_from_directory
from werkzeug.utils import secure_filename

app = Flask(__name__)

STORAGE_DIR = os.environ.get("VDRIVE_STORAGE", "backup_storage")
os.makedirs(STORAGE_DIR, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _index_path() -> str:
    return os.path.join(STORAGE_DIR, "index.json")


def _load_index() -> dict:
    p = _index_path()
    if not os.path.exists(p):
        return {}
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_index(index: dict) -> None:
    with open(_index_path(), "w") as f:
        json.dump(index, f, indent=2)


def _safe_remove(path: str) -> None:
    try:
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return "OK", 200


@app.route("/store", methods=["POST"])
def store():
    f = request.files.get("file")
    if not f:
        return {"ok": False, "error": "no file"}, 400
    filename = secure_filename(f.filename)
    f.save(os.path.join(STORAGE_DIR, filename))
    return {"ok": True}


@app.route("/store_index", methods=["POST"])
def store_index():
    f = request.files.get("file")
    if not f:
        return {"ok": False, "error": "no file"}, 400
    f.save(_index_path())
    return {"ok": True}


@app.route("/list")
def list_files():
    return jsonify(list(_load_index().keys()))


@app.route("/list_parts")
def list_parts():
    try:
        return jsonify([
            fname for fname in os.listdir(STORAGE_DIR)
            if os.path.isfile(os.path.join(STORAGE_DIR, fname))
        ])
    except Exception:
        return jsonify([])


@app.route("/get_part/<path:filename>")
def get_part(filename: str):
    filename = secure_filename(filename)
    return send_from_directory(STORAGE_DIR, filename, as_attachment=True)


@app.route("/get_index")
def get_index():
    return send_from_directory(STORAGE_DIR, "index.json", as_attachment=True)


@app.route("/delete/<path:filename>", methods=["DELETE"])
def delete(filename: str):
    """
    Remove logical entry from index AND delete all associated part files.
    Also sweeps for loose part files matching the name pattern.
    """
    filename = secure_filename(filename)
    index = _load_index()
    entry = index.pop(filename, None)
    _save_index(index)

    # Delete parts listed in index entry
    if isinstance(entry, dict):
        for part_path in entry.get("parts", []):
            _safe_remove(os.path.join(STORAGE_DIR, secure_filename(os.path.basename(part_path))))

    # Sweep for loose matching files
    try:
        for fname in os.listdir(STORAGE_DIR):
            if fname.startswith(filename + ".part") or fname == filename:
                _safe_remove(os.path.join(STORAGE_DIR, fname))
    except Exception:
        pass

    return {"ok": True}


@app.route("/info")
def info():
    try:
        files = [
            f for f in os.listdir(STORAGE_DIR)
            if os.path.isfile(os.path.join(STORAGE_DIR, f))
        ]
        size = sum(os.path.getsize(os.path.join(STORAGE_DIR, f)) for f in files)
        return jsonify({
            "file_count": len(files),
            "size_mb": round(size / (1024 * 1024), 2),
        })
    except Exception:
        return jsonify({"file_count": 0, "size_mb": 0})


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("VDRIVE_PORT", 5000))
    print(f"[vdrive backup node] Listening on 0.0.0.0:{port}")
    print(f"[vdrive backup node] Storage: {os.path.abspath(STORAGE_DIR)}")
    app.run(host="0.0.0.0", port=port)
