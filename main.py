"""
DRP Pipeline - Main entry point.

A modular pipeline for collecting data from various sources, e.g. government websites
and uploading to various repositories, e.g. DataLumos.
"""

import sys

import click

from utils.Args import Args
from utils.Logger import Logger


_HELP_TEXT = """\
DRP Pipeline — modular data collection and upload pipeline

USAGE
  python main.py <module> [options]

MODULES
  setup                 Check/create config.json and google-credentials.json (start here)
  sourcing              Pull candidate URLs from the Google Sheet into the database
  socrata_collector     Download datasets from Socrata (data.gov) sources
  catalog_collector     Download datasets from catalog.data.gov sources
  cms_collector         Download datasets from data.cms.gov sources
  interactive_collector Browser-assisted collector for sites that block automation
  upload                Upload collected datasets to DataLumos
  publisher             Update the Google Sheet with DataLumos links
  cleanup_inprogress    Reset stuck in-progress records
  noop                  No-op (useful for testing config)
  help                  Show this help message

COMMON OPTIONS
  -c, --config PATH       Config file (default: ./config.json)
  -n, --num-rows INT      Max projects per batch
  --start-drpid INT       Only process projects with DRPID >= this value
  -l, --log-level LEVEL   DEBUG, INFO, WARNING, ERROR (default: INFO)
  --log-color             Colorize log severity in terminal

FIRST TIME?
  Run setup to create your configuration files and verify everything works:

    python main.py setup

  For full documentation:
    docs/Setup.md         Installation and configuration
    docs/Usage.md         All usage options

CHAT INTERFACE (recommended)
  The pipeline ships with an MCP server that lets Claude drive the pipeline
  interactively — inspecting state, running modules with dry-run previews,
  fixing errors, and verifying results — without memorizing command-line flags.

  See docs/Usage.md §1 "MCP Orchestration" for setup instructions.
"""


def print_help() -> None:
    print(_HELP_TEXT)


def setup() -> None:
    """
    Initialize the application: configuration and logging.

    Note: Args must be initialized before Logger since Logger configuration
    comes from Args. Args uses print() for warnings, not Logger, so this order is safe.
    """
    # Initialize configuration (handles command line args and config file)
    Args.initialize()

    # Initialize logger using config (Args already initialized above)
    log_level = Args.log_level
    log_color = getattr(Args, "log_color", False)
    Logger.initialize(log_level=log_level, log_color=log_color)

    Logger.info("DRP Pipeline starting...")
    Logger.debug(f"Python version: {sys.version}")

    # Log configuration info
    if Args.config_file:
        Logger.info(f"Using config file: {Args.config_file}")
    Logger.info(f"Log level: {log_level}")


def main() -> None:
    """Main entry point for the DRP Pipeline application."""
    setup()

    module = Args.module

    if module is None:
        print_help()
        sys.stdout.flush()
        print("Error: missing required argument MODULE.", file=sys.stderr)
        sys.exit(1)

    if module == "help":
        print_help()
        return

    from orchestration import Orchestrator

    Orchestrator.run(module)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise  # Preserve exit code from --help etc.
    except click.ClickException as e:
        print(e.format_message(), file=sys.stderr)
        sys.exit(e.exit_code)
