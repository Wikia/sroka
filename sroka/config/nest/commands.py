import os
import shutil
from argparse import ArgumentParser as _ArgumentParser
from pathlib import Path
from typing import Callable

_acts = {}


def _action(func: Callable[[bool], None]) -> Callable[[bool], None]:
    _acts[func.__name__] = func
    return func


@_action
def template(force: bool):
    try:
        template = Path(__file__).parent.joinpath("templates", ".sroka_config").resolve()
        shutil.copytree(
            str(template),
            str(Path(os.getcwd()).joinpath(".sroka_config").resolve()),
            dirs_exist_ok=force
        )
    except FileExistsError:
        print("I couldn't create the folder, because .sroka_config already exists in this directory.",
              "Override this using the --force argument")
    except shutil.Error as err:
        for src, _, e in err.args[0]:
            print(
                f"When copying {Path(src).relative_to(template.parent)} I encountered an exception:",
                e,
                sep="\n"
            )
    else:
        print(f"I created the config folder in {os.getcwd()}")


@_action
def get(force: bool):
    pass


@_action
def set(force: bool):
    pass


_parser = _ArgumentParser(
    prog="sroka nest",
    description="""
    Sroka's Neat Environment Setup Tool

    Use it to mmanage your config files
    """,
)

_parser.add_argument(
    "action",
    choices=_acts.keys()
)

_parser.add_argument(
    "--force", "-f",
    action="store_true"
)


def perform(arg_list: list[str]) -> None:
    args = _parser.parse_args(arg_list)

    try:
        action = _acts[args.action]
    except KeyError:
        _parser.print_help()
    else:
        action(args.force)
