from __future__ import annotations

import platform
import sys
from pathlib import Path


def main() -> int:
    # On this Windows machine, Streamlit import can hang in platform._wmi_query.
    # Force a deterministic platform string before importing streamlit internals.
    if platform.system().lower().startswith("win"):
        platform.system = lambda: "Windows"  # type: ignore[assignment]

    from streamlit.web import cli as stcli

    root = Path(__file__).resolve().parents[1]
    app_path = root / "app.py"

    args = [
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        "127.0.0.1",
        "--server.port",
        "8501",
    ]

    extra_args = sys.argv[1:]
    if extra_args:
        args.extend(extra_args)

    sys.argv = args
    return stcli.main()


if __name__ == "__main__":
    raise SystemExit(main())
