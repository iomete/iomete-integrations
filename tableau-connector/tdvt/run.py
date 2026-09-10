#!/usr/bin/env python3
"""Set up or run Tableau TDVT against a live IOMETE cluster."""

import argparse
import subprocess
import sys

from helpers import TdvtError, run_tdvt, setup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "run"))
    args = parser.parse_args()

    try:
        {"setup": setup, "run": run_tdvt}[args.command]()
    except TdvtError as error:
        parser.error(str(error))
    except subprocess.CalledProcessError as error:
        return error.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
