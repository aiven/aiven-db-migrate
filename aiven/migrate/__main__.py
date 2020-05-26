# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from .pgmigrate import main as pg_main


def pg(args_):
    pg_main(args_, prog="pg")


if __name__ == "__main__":
    from typing import Optional
    import sys

    commands = ("pg", )
    args = sys.argv[1:]
    c: Optional[str]
    if args:
        c = args.pop(0)
    else:
        c = None

    if not c or c not in commands:
        print("Available commands: {}".format(", ".join(commands)))
        sys.exit(1)

    sys.exit(locals()[c](args))
