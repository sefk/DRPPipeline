"""
Interactive setup wizard for DRP Pipeline.

Guides the user through creating config.json and google-credentials.json,
then validates each file independently and together.

Run via:
    python main.py setup
"""

import getpass
import json
import sys
from pathlib import Path
from typing import Optional


# ── Terminal colors ────────────────────────────────────────────────────────────

def _is_tty() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

GREEN  = "\033[92m" if _is_tty() else ""
YELLOW = "\033[93m" if _is_tty() else ""
RED    = "\033[91m" if _is_tty() else ""
BOLD   = "\033[1m"  if _is_tty() else ""
RESET  = "\033[0m"  if _is_tty() else ""

OK   = f"{GREEN}✓{RESET}"
WARN = f"{YELLOW}~{RESET}"
FAIL = f"{RED}✗{RESET}"


def _section(title: str) -> None:
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}{title}{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}")


def _prompt(label: str, default: str = "", secret: bool = False) -> str:
    """Prompt the user for input; show default in brackets."""
    if default:
        prompt_str = f"  {label} [{default}]: "
    else:
        prompt_str = f"  {label}: "
    try:
        if secret:
            value = getpass.getpass(prompt_str)
        else:
            value = input(prompt_str).strip()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    return value if value else default


def _confirm(label: str, default: bool = True) -> bool:
    default_str = "Y/n" if default else "y/N"
    try:
        answer = input(f"  {label} [{default_str}]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    if not answer:
        return default
    return answer in ("y", "yes")


# ── Required config keys and service account keys ─────────────────────────────

_REQUIRED_CONFIG_KEYS = [
    "db_path",
    "base_output_dir",
    "datalumos_username",
    "datalumos_password",
    "google_sheet_id",
    "google_credentials",
    "google_sheet_name",
    "google_username",
    "gwda_your_name",
]

_REQUIRED_CREDS_KEYS = [
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "auth_uri",
    "token_uri",
]


# ── Config setup ───────────────────────────────────────────────────────────────

_DEFAULT_CONFIG_PATH = Path("./config.json")


def _setup_config() -> tuple[bool, Path]:
    """
    Check for config.json; create it interactively if missing.

    Returns (config_existed_or_created, config_path).
    """
    _section("Step 1: config.json")

    config_path = _DEFAULT_CONFIG_PATH
    if config_path.exists():
        print(f"  {OK}  config.json found: {config_path.resolve()}")
        return True, config_path

    print(f"  {WARN}  config.json not found. Let's create one.\n")
    print("  Press Enter to accept defaults shown in [brackets].\n")

    db_path     = _prompt("Database path (db_path)", "drp_pipeline.db")
    output_dir  = _prompt("Download directory (base_output_dir)")
    dl_user     = _prompt("DataLumos username (email)")
    dl_pass     = _prompt("DataLumos password", secret=True)
    sheet_id    = _prompt("Google Sheet ID (from the URL)")
    sheet_name  = _prompt("Google Sheet tab name (google_sheet_name)", "CDC")
    creds_path  = _prompt("Path to google-credentials.json", "google-credentials.json")
    guser       = _prompt("Your username for 'Claimed' column (google_username)")
    gwda_name   = _prompt("Your full name for GWDA nomination (gwda_your_name)")
    url_prefix  = _prompt("URL prefix filter for sourcing (leave blank for none)")
    num_rows    = _prompt("Max rows per batch (num_rows, blank for unlimited)")
    log_level   = _prompt("Log level", "INFO")

    cfg: dict = {
        "log_level":              log_level or "INFO",
        "db_path":                db_path,
        "storage_implementation": "StorageSQLLite",
        "base_output_dir":        output_dir,
        "datalumos_username":     dl_user,
        "datalumos_password":     dl_pass,
        "upload_headless":        True,
        "upload_timeout":         60000,
        "google_sheet_id":        sheet_id,
        "google_credentials":     creds_path,
        "google_sheet_name":      sheet_name,
        "google_username":        guser,
        "gwda_your_name":         gwda_name,
    }
    if url_prefix:
        cfg["sourcing_url_prefix"] = url_prefix
    if num_rows and num_rows.isdigit():
        cfg["num_rows"] = int(num_rows)

    print(f"\n  Will write config.json with these values:")
    for k, v in cfg.items():
        display = "***" if k == "datalumos_password" else repr(v)
        print(f"    {k}: {display}")

    if not _confirm("\n  Write config.json?"):
        print(f"  {WARN}  Skipped — config.json not written.")
        return False, config_path

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")
    print(f"  {OK}  config.json written to {config_path.resolve()}")
    return True, config_path


# ── Google credentials setup ───────────────────────────────────────────────────

def _setup_credentials(config_path: Path) -> tuple[bool, Optional[Path]]:
    """
    Check for the credentials file; walk user through provisioning if missing.

    Reads google_credentials path from config if available.
    Returns (ok, creds_path).
    """
    _section("Step 2: google-credentials.json")

    # Try to read creds path from config
    creds_path = _resolve_creds_path(config_path)
    if creds_path is None:
        creds_path = Path("google-credentials.json")

    if creds_path.exists():
        print(f"  {OK}  Credentials file found: {creds_path.resolve()}")
        return True, creds_path

    print(f"  {WARN}  Credentials file not found: {creds_path}")
    print()
    print("  Google service account credentials are required for:")
    print("    • Sourcing (resolving sheet tab name)")
    print("    • Publisher (updating the spreadsheet)")
    print()
    print("  Follow these steps to create a service account key:\n")
    print("  1. Go to https://console.cloud.google.com/")
    print("     Create a project or select an existing one.")
    print()
    print("  2. Enable the Google Sheets API:")
    print("     APIs & Services → Library → search 'Google Sheets API' → Enable")
    print()
    print("  3. Create a Service Account:")
    print("     APIs & Services → Credentials → Create Credentials → Service Account")
    print("     Give it a name (e.g. 'drp-pipeline') → Create and Continue → Done")
    print()
    print("  4. Download the JSON key:")
    print("     Click your service account → Keys tab → Add Key → Create new key → JSON")
    print("     Save the downloaded file.")
    print()
    print("  5. Share your Google Sheet with the service account:")
    print("     Open the sheet → Share → add the service account email (client_email)")
    print("     from the JSON file → give Editor permissions → Send")
    print()

    if not _confirm("  Have you downloaded the JSON key file?", default=False):
        print(f"  {WARN}  Skipping credentials setup.")
        return False, None

    src = _prompt(f"  Path to downloaded JSON file", str(creds_path))
    src_path = Path(src).expanduser()
    if not src_path.exists():
        print(f"  {FAIL}  File not found: {src_path}")
        return False, None

    dest = creds_path
    if src_path.resolve() != dest.resolve():
        import shutil
        shutil.copy2(src_path, dest)
        print(f"  {OK}  Copied to {dest.resolve()}")
    else:
        print(f"  {OK}  File is already in place: {dest.resolve()}")

    return True, dest


def _resolve_creds_path(config_path: Path) -> Optional[Path]:
    """Read google_credentials from config file, return Path or None."""
    if not config_path.exists():
        return None
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
        p = cfg.get("google_credentials")
        return Path(p) if p else None
    except Exception:
        return None


# ── Validation ─────────────────────────────────────────────────────────────────

def _validate_config(config_path: Path) -> list[tuple[str, str, str]]:
    """
    Validate config.json independently.

    Returns list of (check_name, status, detail) — status is OK, WARN, or FAIL.
    """
    checks: list[tuple[str, str, str]] = []

    if not config_path.exists():
        checks.append(("config.json exists", "FAIL", f"Not found: {config_path}"))
        return checks
    checks.append(("config.json exists", "OK", str(config_path.resolve())))

    # Parse JSON
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
        checks.append(("config.json is valid JSON", "OK", ""))
    except json.JSONDecodeError as e:
        checks.append(("config.json is valid JSON", "FAIL", str(e)))
        return checks

    # Required keys
    missing = [k for k in _REQUIRED_CONFIG_KEYS if not cfg.get(k)]
    if missing:
        checks.append(("Required keys present", "FAIL", f"Missing: {', '.join(missing)}"))
    else:
        checks.append(("Required keys present", "OK", ""))

    # base_output_dir exists (warn if not, it may be created later)
    output_dir = cfg.get("base_output_dir", "")
    if output_dir:
        p = Path(output_dir)
        if p.exists():
            checks.append(("base_output_dir exists", "OK", str(p)))
        else:
            checks.append(("base_output_dir exists", "WARN",
                            f"{p} — will be created when pipeline runs"))
    else:
        checks.append(("base_output_dir set", "FAIL", "base_output_dir is empty"))

    # google_credentials path (existence checked in combined validation)
    creds = cfg.get("google_credentials", "")
    if creds:
        checks.append(("google_credentials path set", "OK", creds))
    else:
        checks.append(("google_credentials path set", "WARN",
                        "Not set — sourcing and publisher will use unauthenticated mode"))

    return checks


def _validate_credentials(creds_path: Optional[Path]) -> list[tuple[str, str, str]]:
    """Validate google-credentials.json independently."""
    checks: list[tuple[str, str, str]] = []

    if creds_path is None:
        checks.append(("Credentials path known", "WARN",
                        "google_credentials not set in config — skipping"))
        return checks

    if not creds_path.exists():
        checks.append(("Credentials file exists", "FAIL", f"Not found: {creds_path}"))
        return checks
    checks.append(("Credentials file exists", "OK", str(creds_path.resolve())))

    try:
        with open(creds_path, encoding="utf-8") as f:
            creds = json.load(f)
        checks.append(("Credentials file is valid JSON", "OK", ""))
    except json.JSONDecodeError as e:
        checks.append(("Credentials file is valid JSON", "FAIL", str(e)))
        return checks

    # Required keys
    missing = [k for k in _REQUIRED_CREDS_KEYS if not creds.get(k)]
    if missing:
        checks.append(("Required credential keys present", "FAIL",
                        f"Missing: {', '.join(missing)}"))
    else:
        checks.append(("Required credential keys present", "OK", ""))

    # type == "service_account"
    if creds.get("type") == "service_account":
        checks.append(("type is service_account", "OK", ""))
    else:
        checks.append(("type is service_account", "FAIL",
                        f"Got: {creds.get('type')!r}"))

    # client_email present (also used for sharing the sheet)
    client_email = creds.get("client_email", "")
    if client_email:
        checks.append(("client_email present", "OK", client_email))
    else:
        checks.append(("client_email present", "FAIL", "Missing client_email"))

    return checks


def _validate_together(config_path: Path, creds_path: Optional[Path]) -> list[tuple[str, str, str]]:
    """Validate config and credentials together (sheet access via API)."""
    checks: list[tuple[str, str, str]] = []

    if not config_path.exists():
        checks.append(("Combined validation", "FAIL", "config.json missing"))
        return checks

    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        checks.append(("Combined validation", "FAIL", f"Cannot read config: {e}"))
        return checks

    # credentials path in config matches actual file
    cfg_creds = cfg.get("google_credentials", "")
    if cfg_creds:
        cfg_creds_path = Path(cfg_creds)
        if cfg_creds_path.resolve() == (creds_path.resolve() if creds_path else None):
            checks.append(("Credentials path in config matches file", "OK", str(cfg_creds_path)))
        elif cfg_creds_path.exists():
            checks.append(("Credentials path in config matches file", "OK",
                            f"Config points to {cfg_creds_path}"))
            creds_path = cfg_creds_path  # use what config says
        else:
            checks.append(("Credentials path in config matches file", "FAIL",
                            f"Config points to {cfg_creds_path} which does not exist"))
    else:
        checks.append(("Credentials path in config", "WARN",
                        "google_credentials not set — skipping API check"))
        return checks

    if creds_path is None or not creds_path.exists():
        checks.append(("Sheet API access", "FAIL", "Credentials file not found"))
        return checks

    # Try Sheets API to resolve tab name
    sheet_id   = cfg.get("google_sheet_id", "")
    sheet_name = cfg.get("google_sheet_name", "")

    if not sheet_id:
        checks.append(("google_sheet_id set", "FAIL", "Missing google_sheet_id"))
        return checks
    checks.append(("google_sheet_id set", "OK", sheet_id))

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        checks.append(("Google API libraries installed", "FAIL",
                        "pip install google-auth google-api-python-client"))
        return checks
    checks.append(("Google API libraries installed", "OK", ""))

    try:
        creds_obj = service_account.Credentials.from_service_account_file(
            str(creds_path),
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds_obj)
        meta = (
            service.spreadsheets()
            .get(
                spreadsheetId=sheet_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
        )
        tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]
        checks.append(("Google Sheet accessible", "OK",
                        f"Found {len(tabs)} tab(s): {', '.join(tabs[:5])}"))

        if sheet_name:
            if sheet_name in tabs:
                checks.append(("Sheet tab found", "OK",
                                f"Tab '{sheet_name}' exists in the spreadsheet"))
            else:
                checks.append(("Sheet tab found", "FAIL",
                                f"Tab '{sheet_name}' not found. Available: {', '.join(tabs)}"))
        else:
            checks.append(("google_sheet_name set", "WARN",
                            "Not set — first sheet will be used"))

    except Exception as e:
        msg = str(e)
        if "403" in msg:
            try:
                with open(creds_path, encoding="utf-8") as f:
                    email = json.load(f).get("client_email", "")
            except Exception:
                email = "(unknown)"
            checks.append(("Google Sheet accessible", "FAIL",
                            f"Permission denied (403). Share the sheet with: {email}"))
        elif "404" in msg:
            checks.append(("Google Sheet accessible", "FAIL",
                            f"Sheet not found (404). Check google_sheet_id: {sheet_id}"))
        else:
            checks.append(("Google Sheet accessible", "FAIL", msg))

    return checks


# ── Report printer ─────────────────────────────────────────────────────────────

def _print_checks(checks: list[tuple[str, str, str]]) -> bool:
    """Print checks; return True if no FAILs."""
    ok = True
    for name, status, detail in checks:
        if status == "OK":
            icon = OK
        elif status == "WARN":
            icon = WARN
        else:
            icon = FAIL
            ok = False
        suffix = f"  — {detail}" if detail else ""
        print(f"  {icon}  {name}{suffix}")
    return ok


# ── Main class ─────────────────────────────────────────────────────────────────

class Setup:
    """Interactive setup wizard for DRP Pipeline configuration."""

    def run(self, drpid: int) -> None:  # noqa: ARG002
        print(f"\n{BOLD}DRP Pipeline Setup{RESET}")
        print("Checks and creates config.json and google-credentials.json.\n")

        # Step 1: Ensure config.json exists
        _config_ok, config_path = _setup_config()

        # Step 2: Ensure credentials file exists
        creds_path = _resolve_creds_path(config_path)
        _creds_ok, creds_path = _setup_credentials(config_path)

        # Step 3: Validate config.json
        _section("Validation: config.json")
        config_checks = _validate_config(config_path)
        config_valid = _print_checks(config_checks)

        # Step 4: Validate credentials
        _section("Validation: google-credentials.json")
        creds_checks = _validate_credentials(creds_path)
        creds_valid = _print_checks(creds_checks)

        # Step 5: Combined validation (sheet access)
        _section("Validation: Sheet access")
        combined_checks = _validate_together(config_path, creds_path)
        combined_valid = _print_checks(combined_checks)

        # Summary
        _section("Summary")
        all_ok = config_valid and creds_valid and combined_valid
        if all_ok:
            print(f"  {OK}  {GREEN}All checks passed. Pipeline is ready to run.{RESET}")
        else:
            failed = []
            if not config_valid:
                failed.append("config.json")
            if not creds_valid:
                failed.append("google-credentials.json")
            if not combined_valid:
                failed.append("sheet access")
            print(f"  {FAIL}  {RED}Issues found in: {', '.join(failed)}{RESET}")
            print(f"       Fix the items marked {RED}✗{RESET} above, then re-run: python main.py setup")
        print()
