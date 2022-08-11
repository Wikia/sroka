import os
from functools import cache
from pathlib import Path

from .commands import template

_GLOBAL_PATH = os.path.expanduser('~/.sroka_config/')
_COMPATIBLE = ("posix")


def NEST(file: str) -> str:
    return get_config(file)


@cache
def get_config(file: str) -> str:
    if not Path(os.getcwd()).joinpath('.sroka_config', file).is_file():
        print("I couldn't find", file, "in the local config directory.")

        if os.name.lower() in _COMPATIBLE:
            print("I'll try to use the global config file")
            if Path(_GLOBAL_PATH).joinpath(file).is_file():
                print("I successfully retrieved", file, "from the global config")
                return Path(_GLOBAL_PATH).joinpath(file).resolve()
            print("Global config for this file doesn't exist")

        else:
            print("I can't use global configs on this operating system.")

        print("I'm creating a local config from template.")
        template()
        if not Path(os.getcwd()).joinpath('.sroka_config', file).is_file():
            print("Local config from template didn't help,",
                  "please create the file manually according to our documentation.")
            exit()

    return str(Path(os.getcwd()).joinpath('.sroka_config').resolve())
