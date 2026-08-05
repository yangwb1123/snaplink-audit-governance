import sys
from .acceptance import run


def main() -> int:
    result = run()
    print("health: PASS" if result == 0 else "health: FAIL")
    return result


if __name__ == "__main__":
    sys.exit(main())

