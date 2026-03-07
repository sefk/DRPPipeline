"""
DRP Pipeline - Main entry point.

A modular pipeline for collecting data from various sources, e.g. government websites
and uploading to various repositories, e.g. DataLumos.
"""

import sys

import click

from utils.Args import Args
from utils.Logger import Logger


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

    from orchestration import Orchestrator

    Orchestrator.run(Args.module)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise  # Preserve exit code from --help etc.
    except click.ClickException as e:
        print(e.format_message(), file=sys.stderr)
        sys.exit(e.exit_code)
