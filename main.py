from __future__ import annotations

import logging

import ttkbootstrap as tb

from config import AppConfig, LOG_FILE
from ui.app import App


def main():
    cfg = AppConfig.load()
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    root = tb.Window(themename=cfg.theme)
    app  = App(root)
    root.protocol("WM_DELETE_WINDOW", lambda: (app.close(), root.destroy()))
    root.mainloop()


if __name__ == "__main__":
    main()
