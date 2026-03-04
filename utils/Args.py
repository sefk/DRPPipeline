"""
Command line arguments and configuration with singleton pattern.

Provides a centralized configuration accessible via direct attribute access.
Supports both command line arguments and config file values.

Config File Format:
    JSON format with simple key-value pairs.
    
    Example config.json:
    {
        "log_level": "DEBUG",
        "config_file": "config.json"
    }

Example usage:
    from utils.Args import Args
    
    # Initialize with optional config file
    Args.initialize()
    
    # Access configuration values as attributes
    log_level = Args.log_level  # From --log-level, config file, or defaults
    config_file = Args.config_file  # From --config or config file

Note: Priority order (highest to lowest):
    1. Command line arguments (from Typer)
    2. Config file values
    3. Default values (from defaults dict)
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import typer


class ArgsMeta(type):
    """Metaclass to provide direct attribute access to config values."""
    
    def __getattr__(cls, name: str):
        """Provide attribute access to config values."""
        if not cls._initialized:
            raise RuntimeError("Args has not been initialized. Call Args.initialize() first.")
        
        # Check config dict (command line overrides already applied)
        if name in cls._config:
            return cls._config[name]
        
        raise AttributeError(f"Config item '{name}' not found")


class Args(metaclass=ArgsMeta):
    """Args class providing direct attribute access to configuration."""
    
    # Default values (lowest priority)
    _defaults: Dict[str, Any] = {
        "config_file": None,  # Set from --config when provided
        "log_level": "INFO",
        "log_color": True,  # Set True with --log-color to color severity in terminal (only when TTY)
        "sourcing_spreadsheet_url": (
            "https://docs.google.com/spreadsheets/d/1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY/edit?gid=101637367#gid=101637367"
        ),
        "sourcing_url_column": "URL",
        "sourcing_fetch_timeout": 15,  # Seconds per URL when checking availability in sourcing; reduces delay from slow catalog.data.gov pages
        "num_rows": None,  # None = unlimited; batch limit for orchestration
        "start_row": None,  # If set, skip first (start_row - 1) rows (1-origin); used when listing from DB
        "start_drpid": None,  # If set, only projects with DRPID >= start_drpid (overrides start_row when set)
        "db_path": 'drp_pipeline.db',
        "storage_implementation": "StorageSQLLite",
        "base_output_dir": r"C:\Documents\DataRescue\DRPData",
        "delete_all_db_entries": False,
        "max_workers": 1,  # Parallel projects when > 1 (e.g. collector); 1 = sequential
        "download_timeout_ms": 30 * 60 * 1000,  # 30 min for large datasets; increase for 10GB+
        "use_url_download": True,  # Get URL from Playwright then download with requests (progress/resume)
        "socrata_app_token": None,  # Optional; set in config for direct Socrata API download (avoids 403)
        # Upload module settings
        "datalumos_username": None,  # Required for upload; set in config file
        "datalumos_password": None,  # Required for upload; set in config file
        "upload_headless": False,  # Run browser in headless mode for upload
        "upload_timeout": 60000,  # Default timeout in ms for upload operations
        # GWDA nomination (before DataLumos upload)
        "gwda_your_name": "Michael Kraley",
        "gwda_institution": "Data Rescue Project",
        "gwda_email": None,  # Uses datalumos_username if not set
        # Publisher: Google Sheet update (optional; after publish)
        "google_sheet_id": None,  # Google Sheet ID from URL; set in config to enable
        "google_credentials": None,  # Path to service account JSON; required if google_sheet_id set
        "google_sheet_name": "CDC",  # Worksheet/tab name
        "google_username": "mkraley",  # Value for "Claimed" column
        "stop_file": None,  # When set (e.g. via env DRP_STOP_FILE), orchestrator checks this path each iteration and exits if file exists
    }
    
    _config: Dict[str, Any] = {}
    _initialized: bool = False
    _app: Optional[typer.Typer] = None
    _parsed_args: Dict[str, Any] = {}

    @classmethod
    def initialize(cls, config_file: Optional[Path] = None) -> None:
        """
        Initialize configuration from defaults, config file, and command line args.
        
        Priority order (highest to lowest):
            1. Command line arguments
            2. Config file values
            3. Default values
        
        Args:
            config_file: Optional path to config file. If None, uses --config from command line or no config file.
        """
        if cls._initialized:
            return
        
        # Start with defaults (lowest priority)
        cls._config = dict(cls._defaults)
        
        # Parse command line arguments first to get config file path if not provided
        parsed_args = cls._parse_command_line()
        
        # Load config file if provided (middle priority - overrides defaults)
        # Default to "./config.json" if not explicitly provided
        config_path = config_file or parsed_args.get("config")
        if config_path is None:
            config_path = "./config.json"
        
        if config_path:
            if not isinstance(config_path, Path):
                config_path = Path(config_path)
            if config_path.exists():
                cls._load_config_file(config_path)
            else:
                # Warn if config file not found, but continue without it
                print(f"Warning: Config file '{config_path}' not found. Using defaults and command line arguments only.",
                      file=sys.stderr)
        
        # Apply command line arguments (highest priority - overrides config file and defaults)
        cls._apply_command_line_args(parsed_args)

        # config_file: same as config (--config) for code that reads Args.config_file (stored as str)
        if "config" in cls._config and cls._config["config"] is not None:
            cls._config["config_file"] = str(cls._config["config"])

        # gwda_email fallback: use datalumos_username if gwda_email not set
        gwda_email = cls._config.get("gwda_email") or cls._config.get("datalumos_username")
        cls._config["gwda_email"] = gwda_email

        # stop_file from environment (set by API when running pipeline from GUI so orchestrator can check for stop request)
        if os.environ.get("DRP_STOP_FILE"):
            cls._config["stop_file"] = os.environ["DRP_STOP_FILE"]

        cls._initialized = True

    @classmethod
    def initialize_from_config(cls, config_path: Optional[Path] = None) -> None:
        """
        Initialize from defaults and config file only (no CLI parsing).
        Use when the app is run without the Typer CLI (e.g. Flask, pytest).
        """
        if cls._initialized:
            return
        cls._config = dict(cls._defaults)
        if config_path is None:
            config_path = Path("./config.json")
        if not isinstance(config_path, Path):
            config_path = Path(config_path)
        if config_path.exists():
            cls._load_config_file(config_path)
        gwda_email = cls._config.get("gwda_email") or cls._config.get("datalumos_username")
        cls._config["gwda_email"] = gwda_email
        if os.environ.get("DRP_STOP_FILE"):
            cls._config["stop_file"] = os.environ["DRP_STOP_FILE"]
        cls._initialized = True

    @classmethod
    def _parse_command_line(cls) -> Dict[str, Any]:
        """
        Parse command line arguments using Typer.
        
        Returns:
            Dictionary of parsed command line arguments
        """
        # Store parsed values in a dict that can be accessed from the callback
        parsed_values: Dict[str, Any] = {}
        
        def callback(
            ctx: typer.Context,
            module: str = typer.Argument(..., help="Module to run: noop, sourcing, collector, upload, publisher, cleanup_inprogress, interactive_collector"),
            config: Optional[Path] = typer.Option(None, "--config", "-c", help="Path to configuration file (JSON format). Default: ./config.json"),
            log_level: Optional[str] = typer.Option(None, "--log-level", "-l", help="Set the logging level", case_sensitive=False),
            num_rows: Optional[int] = typer.Option(None, "--num-rows", "-n", help="Max projects or candidate URLs per batch; None = unlimited"),
            start_row: Optional[int] = typer.Option(None, "--start-row", help="Start at this 1-origin row (all rows, ORDER BY DRPID); skip earlier rows"),
            start_drpid: Optional[int] = typer.Option(None, "--start-drpid", help="Only process projects with DRPID >= this value"),
            db_path: Optional[Path] = typer.Option(None, "--db-path", help="Path to SQLite database file"),
            storage: Optional[str] = typer.Option(None, "--storage", help="Storage implementation (e.g. StorageSQLLite)"),
            delete_all_db_entries: bool = typer.Option(False, "--delete-all-db-entries", help="Delete all database entries and reset auto-increment before proceeding"),
            max_workers: Optional[int] = typer.Option(None, "--max-workers", "-w", help="Max concurrent projects for modules with prereq (e.g. collector). Default 1 = sequential."),
            download_timeout_ms: Optional[int] = typer.Option(None, "--download-timeout-ms", help="Download timeout in milliseconds (default 30 min). Use for large datasets."),
            no_use_url_download: bool = typer.Option(False, "--no-use-url-download", help="Use Playwright save_as instead of capturing URL and downloading with requests (no progress/resume)."),
            log_color: bool = typer.Option(False, "--log-color", help="Color the log severity in terminal (DEBUG=gray, WARNING=orange, ERROR=red, exception=purple). Only applies when stdout is a TTY."),
        ) -> None:
            """Callback to capture Typer parsed values."""
            parsed_values["module"] = module
            if config is not None:
                parsed_values["config"] = config
            if log_level is not None:
                parsed_values["log_level"] = log_level.upper()
            if num_rows is not None:
                parsed_values["num_rows"] = num_rows
            if start_row is not None:
                parsed_values["start_row"] = start_row
            if start_drpid is not None:
                parsed_values["start_drpid"] = start_drpid
            if db_path is not None:
                parsed_values["db_path"] = db_path
            if storage is not None:
                parsed_values["storage_implementation"] = storage
            if delete_all_db_entries:
                parsed_values["delete_all_db_entries"] = True
            if max_workers is not None:
                parsed_values["max_workers"] = max_workers
            if download_timeout_ms is not None:
                parsed_values["download_timeout_ms"] = download_timeout_ms
            if no_use_url_download:
                parsed_values["use_url_download"] = False
            if log_color:
                parsed_values["log_color"] = True

        # Use a single @app.command() so the first positional (module) is not treated as a
        # subcommand. A Group would require the first token to match a subcommand.
        app = typer.Typer(help="DRP Pipeline - Modular data collection and upload pipeline")
        app.command()(callback)

        # Parse args. With a single Command (no Group), pass args without program name so
        # the first positional (module) is parsed correctly.
        try:
            app(sys.argv[1:], standalone_mode=False)
        except SystemExit:
            raise  # --help/--version: let SystemExit propagate so the process exits

        # If --help was used, the callback is never invoked and module is missing; exit cleanly.
        if "module" not in parsed_values:
            sys.exit(0)

        return parsed_values

    @classmethod
    def _load_config_file(cls, config_path: Path) -> None:
        """
        Load configuration from JSON file and merge into config.
        
        Args:
            config_path: Path to the JSON config file
        """
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_file_data = json.load(f)
                # Merge config file data into existing config (overriding defaults)
                cls._config.update(config_file_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file '{config_path}': {e}")
        except Exception as e:
            raise IOError(f"Error reading config file '{config_path}': {e}")

    @classmethod
    def _apply_command_line_args(cls, parsed_args: Dict[str, Any]) -> None:
        """
        Apply command line argument values to config, overriding file values and defaults.
        
        Args:
            parsed_args: Dictionary of parsed command line arguments from Typer
        """
        # Store parsed args for get_args() method
        cls._parsed_args = parsed_args
      
        # Merge command line args into config (overriding config file and defaults)
        # Only include values that were actually provided (not None)
        for key, value in parsed_args.items():
            if value is not None:
                cls._config[key] = value

    @classmethod
    def get_args(cls) -> Dict[str, Any]:
        """
        Get the parsed command line arguments.
        
        Returns:
            Dictionary containing all command line arguments that were provided
        """
        if not cls._initialized:
            raise RuntimeError("Args has not been initialized. Call Args.initialize() first.")
        return cls._parsed_args.copy()

    @classmethod
    def get_config(cls) -> Dict[str, Any]:
        """
        Get the full configuration dictionary.
        
        Returns:
            Dictionary containing all configuration values
        """
        if not cls._initialized:
            raise RuntimeError("Args has not been initialized. Call Args.initialize() first.")
        return cls._config.copy()
