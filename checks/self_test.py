import sys
from .config import get_config
from .filesize import run as filesize
from .complexity import run as complexity
from .architecture import run as architecture
from .route_contract import run as routes


def run() -> int:
    get_config()
    for check in (filesize, complexity, architecture, routes):
        if check() != 0:
            return 1
    print("PASS: self-test")
    return 0


if __name__ == "__main__":
    sys.exit(run())

