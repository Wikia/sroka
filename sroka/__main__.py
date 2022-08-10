import sys
from argparse import ArgumentParser

from .config.nest.commands import perform as nest_perform

SUBCOMMANDS = {
    "nest": nest_perform
}

parser = ArgumentParser(
    prog="sroka",
    description="Python library for API access and data analysis " +
    "in Product, BI, Revenue Operations (GAM, GA, Athena etc.)",
)

parser.add_argument(
    'subcommand',
    help="Select which component of the library you want to use",
    choices=SUBCOMMANDS.keys()
)

args = parser.parse_args(sys.argv[1:2])

SUBCOMMANDS[args.subcommand](sys.argv[2:])
