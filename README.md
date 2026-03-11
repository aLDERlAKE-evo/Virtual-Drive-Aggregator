# vdrive — Virtual Aggregated Drive Manager

![Python](https://img.shields.io/badge/python-3.10%2B-blue?logo=python&logoColor=white)
![Platform](https://img.shields.io/badge/platform-Windows-lightgrey?logo=windows)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-active-brightgreen)
![Tests](https://img.shields.io/badge/tests-unittest-blue)

> Combine multiple USB drives into a single logical storage volume — with checksums, network backup, pause/resume, and a real-time transfer dashboard.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [How Splitting Works](#how-splitting-works)
- [Getting Started](#getting-started)
- [Running the Backup Node](#running-the-backup-node)
- [Running Tests](#running-tests)
- [Configuration](#configuration)
- [Screenshots](#screenshots)
- [Roadmap](#roadmap)

---

## Overview

**vdrive** lets you treat a collection of small USB flash drives as one unified storage pool. A 4 GB file that won't fit on any single 2 GB drive? vdrive splits it proportionally across all your drives, tracks every part in a replicated index, and reassembles it on demand — with SHA-256 checksum verification at every step.

An optional Flask-based backup node lets you replicate all your parts to another PC on the same LAN, so you have a recovery copy even if a drive fails.

---

## Features

| Category | Feature |
|---|---|
| **Core** | Proportional free-space splitting across 2–4 USB drives |
| **Core** | SHA-256 checksum verification on every part |
| **Core** | Pause / Resume (user-driven and auto drive-removal) |
| **Core** | Cancel and cleanup of in-progress transfers |
| **Core** | Deduplication — skips re-uploading identical files |
| **Reliability** | Pre-flight free-space check before any write begins |
| **Reliability** | Capacity-aware overflow redistribution |
| **Reliability** | Index replicated to all drives on every save |
| **Reliability** | Index versioning — 5 rolling backups per drive |
| **Reliability** | Auto-repair dialog on startup for incomplete files |
| **Reliability** | Resume interrupted uploads using cached part layout |
| **Performance** | Parallel SHA-256 computation across parts |
| **Performance** | Chunked streaming downloads from backup server |
| **UI** | Real-time speed sparkline graph |
| **UI** | Per-file status icons (✅ ⚠️ ❌ 🔄) in file tree |
| **UI** | Live search/filter in the file tree |
| **UI** | Settings dialog (chunk size, compression, theme, workers) |
| **UI** | Drag-and-drop file/folder upload |
| **UI** | Dark/light theme toggle |
| **Backup** | Parallel LAN discovery of backup nodes |
| **Backup** | Push parts + index to any online backup node |
| **Backup** | Restore from backup node back into virtual drives |
| **Safety** | C drive and configurable drive letters are permanently blocked |
| **Safety** | Only `removable` drives are ever offered as options |

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   vdrive GUI (Tkinter)               │
│  ┌──────────┐  ┌───────────┐  ┌───────────────────┐ │
│  │ FileTree │  │ StatusBar │  │   SpeedGraph      │ │
│  │ (icons + │  │ (progress │  │   (sparkline)     │ │
│  │  search) │  │  + ETA)   │  │                   │ │
│  └──────────┘  └───────────┘  └───────────────────┘ │
│              ui/app.py (orchestration)               │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┼─────────────┐
          ▼            ▼             ▼
  ┌──────────────┐ ┌────────┐ ┌──────────────┐
  │   Splitter   │ │ Index  │ │ DriveMonitor │
  │  (split /    │ │Manager │ │ (hot-plug    │
  │   merge /    │ │(load / │ │  detection)  │
  │   checksum)  │ │ save / │ └──────────────┘
  └──────────────┘ │version)│
                   └────────┘
                       │
          ┌────────────┘
          ▼
  ┌──────────────────────┐
  │    BackupClient      │
  │ (discover / send /   │
  │  restore)            │
  └──────────┬───────────┘
             │  HTTP (LAN)
             ▼
  ┌──────────────────────┐
  │   backup/server.py   │
  │   (Flask REST API)   │
  │   runs on backup PC  │
  └──────────────────────┘
```

---

## Project Structure

```
vdrive/
├── main.py                  # Entry point — run this to start the app
├── config.py                # AppConfig dataclass, Status enum, constants
├── requirements.txt
├── README.md
│
├── core/
│   ├── drives.py            # Safe removable drive detection (psutil-based)
│   ├── index.py             # IndexManager — load/save/replicate/versioning
│   └── splitter.py          # Stream split, merge, checksums, space check
│
├── backup/
│   ├── client.py            # BackupClient — discovery, upload, restore
│   └── server.py            # Flask backup node (run on another PC)
│
├── ui/
│   ├── app.py               # Main App window, all transfer orchestration
│   ├── widgets.py           # FileTree, StatusBar, SpeedGraph
│   └── dialogs.py           # SettingsDialog, RepairDialog
│
└── tests/
    ├── test_splitter.py     # distribute_sizes, sha256, checksums
    ├── test_index.py        # IndexManager CRUD, save/load, versioning
    └── test_drives.py       # Drive detection, blocked letters, normalisation
```

---

## How Splitting Works

When you upload a file, vdrive distributes it across your selected drives proportionally to their **available free space**:

```
file_size × (drive_free / total_free_across_all_drives)
```

**Example — 4 GB file, Drive A has 1 GB free, Drive B has 5 GB free:**

| | Drive A | Drive B |
|---|---|---|
| Free space | 1 GB | 5 GB |
| Proportional share | 4 GB × (1/6) = **667 MB** | 4 GB × (5/6) = **3.33 GB** |
| Fits? | ✅ Yes | ✅ Yes |

**Edge case — overflow redistribution:**

If Drive A's proportional share *exceeds* its actual free space (e.g. Drive A has only 100 MB free but would receive 500 MB), the overflow is **redistributed to drives that have headroom**, prioritising the drive with the most available space. This avoids write failures mid-transfer.

**Pre-flight check:**

Before any write begins, vdrive verifies that `total_free_across_all_drives ≥ file_size`. If not, it raises a clear error rather than failing halfway through.

**Resume safety:**

The part-size layout is cached in the index the first time a file is split. If an upload is interrupted and resumed, the **same layout is reused** so parts are never corrupted by a different proportional calculation.

---

## Getting Started

### Prerequisites

- Python 3.10+
- Windows (primary target; Linux/macOS partial support)
- 2–4 USB flash drives

### Installation

```bash
git clone https://github.com/yourusername/vdrive.git
cd vdrive
pip install -r requirements.txt
```

### Running the App

```bash
python main.py
```

### Quick Start

1. Insert at least 2 USB drives.
2. Select them in the **Drive 1–4** dropdowns (only removable drives appear — C: is always blocked).
3. Click **Confirm Drives**.
4. Use **Upload File** or drag files onto the tree.
5. Use **Download** to reassemble a file to your chosen location.

---

## Running the Backup Node

On any other PC on the same LAN:

```bash
pip install flask
python -m backup.server
```

Or with a custom port and storage directory:

```bash
VDRIVE_PORT=5001 VDRIVE_STORAGE=/mnt/backup python -m backup.server
```

Back in the main app, check **Enable Backup** — vdrive will auto-discover the node and push parts to it automatically after every upload.

---

## Running Tests

```bash
# From the project root
python -m pytest tests/ -v

# Or with unittest directly
python -m unittest discover tests
```

**Test coverage:**

| Module | Tests |
|---|---|
| `core/splitter.py` | `distribute_sizes`, `sha256`, parallel checksums, overflow redistribution, pre-flight check |
| `core/index.py` | CRUD, save/load, replication, version rotation, restore, `build_entry` |
| `core/drives.py` | Removable detection, blocked letters, fixed drives excluded, config overrides |

---

## Configuration

Settings are stored in `vdrive_config.json` (auto-created on first run):

```json
{
  "chunk_mb": 8,
  "compression": "store",
  "theme": "darkly",
  "backup_nodes": [],
  "backup_enabled": false,
  "max_workers": 4,
  "blocked_letters": ["C", "D"]
}
```

| Key | Description |
|---|---|
| `chunk_mb` | Read buffer size per write iteration (4–64 MB) |
| `compression` | `"store"` (none), `"zip"` (deflate), `"lzma"` (best ratio) |
| `theme` | Any ttkbootstrap theme name |
| `max_workers` | Background thread pool size |
| `blocked_letters` | Drive letters **never** offered in the UI |

You can also edit all settings from within the app via **⚙ Settings**.

---

## Roadmap

- [ ] End-to-end encryption (AES-256) for parts at rest
- [ ] API key authentication for the backup node
- [ ] Linux / macOS full support (drive detection, `attrib` replacement)
- [ ] Scheduled auto-backup (cron-style)
- [ ] CLI mode for headless operation

---

## License

MIT — see [LICENSE](LICENSE) for details.
