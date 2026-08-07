"""`python -m pbatch` — same entry point as the installed `pi-batch` command
and the repo-root `pi-batch.py` shim."""

from .cli import main

if __name__ == "__main__":
    main()
