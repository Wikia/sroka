from argparse import ArgumentParser as _ArgumentParser
from typing import Callable

_acts = {}


def _action(func: Callable[[str, bool], None]) -> Callable[[str, bool], None]:
    _acts[func.__name__.removeprefix("_")] = func
    return func


@_action
def _template():
    pass


@_action
def _get():
    pass


@_action
def _set():
    pass


_parser = _ArgumentParser(
    prog="sroka nest",
    description="""
    Sroka's Neat Environment Setup Tool

    Use it to mmanage your config files
    """,
)

_parser.add_argument(
    "--force", "f",
    action="store_true"
)


def perform(arg_list: list[str]) -> None:
    args = _parser.parse_args(arg_list)
    print(args)
    # _acts.get()
