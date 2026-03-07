"""
Utilities for file and folder operations.

Provides functions for sanitizing filenames and creating output folders.
"""

import re
import shutil
import unicodedata
from pathlib import Path
from typing import Optional


def sanitize_filename(name: str, max_length: int = 100) -> str:
    """
    Sanitize a filename to be valid for Windows filesystem.
    
    Args:
        name: Original filename
        max_length: Maximum length for the sanitized name
        
    Returns:
        Sanitized filename
        
    Example:
        >>> sanitize_filename("test<file>name")
        'test_file_name'
        >>> sanitize_filename("test–file—name")
        'test-file-name'
    """
    if not name:
        return "Untitled"
    
    # Convert to string and normalize Unicode
    try:
        if isinstance(name, bytes):
            sanitized = name.decode('utf-8', errors='replace')
        else:
            sanitized = str(name)
            sanitized = unicodedata.normalize('NFKD', sanitized)
    except Exception:
        sanitized = str(name)
    
    # Replace common problematic Unicode characters
    replacements = {
        '\u2013': '-',  # en dash
        '\u2014': '-',  # em dash
        '\u2018': "'",  # left single quotation mark
        '\u2019': "'",  # right single quotation mark
        '\u201C': '"',  # left double quotation mark
        '\u201D': '"',  # right double quotation mark
        '\u2026': '...',  # ellipsis
        '\u00A0': ' ',  # non-breaking space
    }
    for old_char, new_char in replacements.items():
        sanitized = sanitized.replace(old_char, new_char)
    
    # Remove invalid Windows characters
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '_', sanitized)
    
    # Remove control characters
    sanitized = re.sub(r'[\x00-\x1f\x7f]', '', sanitized)
    
    # Convert to ASCII
    try:
        sanitized = sanitized.encode('ascii', errors='replace').decode('ascii')
        sanitized = sanitized.replace('?', '_')
    except Exception:
        sanitized = ''.join(c if ord(c) < 128 else '_' for c in sanitized)
    
    # Remove leading/trailing dots and spaces
    sanitized = sanitized.strip('. ')
    
    # Remove multiple consecutive underscores/spaces
    sanitized = re.sub(r'[_\s]+', '_', sanitized)
    sanitized = sanitized.strip('_')
    
    # Limit length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]
        sanitized = sanitized.strip('. _')
    
    if not sanitized:
        sanitized = "Untitled"
    
    return sanitized


def format_file_size(size_bytes: int) -> str:
    """
    Format a byte count in human-readable form (KB, MB, GB).

    Args:
        size_bytes: Size in bytes (non-negative).

    Returns:
        String like "1.2 MB", "500 KB", "3.4 GB".

    Example:
        >>> format_file_size(1536)
        '1.5 KB'
        >>> format_file_size(1_500_000)
        '1.4 MB'
    """
    if size_bytes < 0:
        size_bytes = 0
    for unit, suffix in [(1024**3, "GB"), (1024**2, "MB"), (1024, "KB")]:
        if size_bytes >= unit:
            return f"{size_bytes / unit:.1f} {suffix}"
    return f"{size_bytes} B"


def folder_extensions_and_size(folder_path: Path) -> tuple[list[str], int]:
    """
    Return (sorted list of unique extensions without leading dot, total size in bytes).

    Args:
        folder_path: Path to folder to scan

    Returns:
        Tuple of (extensions list, total bytes). Extensions are lowercased, no leading dot.
    """
    exts: set[str] = set()
    total = 0
    try:
        for p in folder_path.iterdir():
            if p.is_file():
                total += p.stat().st_size
                if p.suffix:
                    exts.add(p.suffix.lstrip(".").lower())
    except OSError:
        pass
    return (sorted(exts), total)


def create_output_folder(base_dir: Path, drpid: int) -> Optional[Path]:
    """
    Create output folder for a DRPID.

    If the folder already exists, it and its contents are removed first.

    Args:
        base_dir: Base directory for creating folders
        drpid: DRPID for the record

    Returns:
        Path to created folder, or None if creation failed

    Example:
        >>> from pathlib import Path
        >>> folder = create_output_folder(Path("/tmp"), 123)
        >>> folder.name
        'DRP000123'
    """
    folder_name = f"DRP{drpid:06d}"
    folder_path = base_dir / folder_name

    try:
        if folder_path.exists():
            try:
                shutil.rmtree(folder_path)
            except OSError as e:
                from utils.Logger import Logger
                Logger.warning(f"Could not empty output folder (in use?): {e}. Using existing folder.")
        folder_path.mkdir(parents=True, exist_ok=True)
        return folder_path
    except Exception as e:
        from utils.Logger import Logger
        Logger.error(f"Failed to create output folder: {e}")
        return None
