"""
DataLumos file upload handler.

Handles uploading files to a DataLumos project using Playwright.
Uses a drop-zone compatible approach: injects a file input and dispatches
drag/drop events to the modal drop target so the UI receives files.

When the output folder contains subfolders, zips all contents and uses
the Import From Zip feature instead of uploading files one-by-one.
"""

import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import List

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from utils.Logger import Logger


# Selectors from DataLumos import file modal (aligned with chiara_upload.py)
UPLOAD_BTN_SELECTOR = "a.btn-primary:nth-child(3) > span:nth-child(4)"
IMPORT_FROM_ZIP_BTN_SELECTOR = 'a.btn-default:has-text("Import From Zip")'
DROP_ZONE_SELECTOR = ".importFileModal .col-md-offset-2 > span:nth-child(1)"
DROP_ZONE_SELECTOR_FROM_ZIP = ".importZipModal .col-md-offset-2 > span:nth-child(1)"
FILE_QUEUED_TEXT = "File added to queue for upload."
# Substring for counting matches inside a single legend / status block (text may omit trailing ".")
FILE_QUEUED_PHRASE = "File added to queue for upload"
FILE_QUEUED_TEXT_FROM_ZIP = "The contents of your ZIP file are being extracted"
# Zip UI may shorten the message; this substring should stay stable.
ZIP_STATUS_SUBSTRING = "being extracted"
CLOSE_MODAL_SELECTOR = ".importFileModal .modal-footer button"
CLOSE_MODAL_SELECTOR_FROM_ZIP = ".importZipModal .modal-footer button"
INJECTED_INPUT_ID = "pw-datalumos-file-input"

# JS to create file input and dispatch drop events to target (adapted from chiara/Selenium approach)
JS_INJECT_FILE_INPUT = """
(selector) => {
  const target = document.querySelector(selector);
  if (!target) return null;
  const input = document.createElement('input');
  input.type = 'file';
  input.id = '""" + INJECTED_INPUT_ID + """';
  input.style.display = 'none';
  input.onchange = function () {
    const rect = target.getBoundingClientRect();
    const x = rect.left + (rect.width >> 1);
    const y = rect.top + (rect.height >> 1);
    const dataTransfer = { files: this.files };
    ['dragenter', 'dragover', 'drop'].forEach(function (name) {
      const evt = document.createEvent('MouseEvent');
      evt.initMouseEvent(name, true, true, window, 0, 0, 0, x, y, false, false, false, false, 0, null);
      Object.defineProperty(evt, 'dataTransfer', { value: dataTransfer });
      target.dispatchEvent(evt);
    });
    this.value = '';
  };
  document.body.appendChild(input);
  return input.id;
}
"""


class DataLumosFileUploader:
    """
    Handles file uploads to DataLumos.
    
    Uses Playwright's set_input_files on an injected file input whose onchange
    dispatches drag/drop events to the modal drop zone.
    """

    def __init__(self, page: Page, timeout: int = 120000, upload_wait_timeout: int = 600000) -> None:
        """
        Initialize the file uploader.

        Args:
            page: Playwright Page object
            timeout: Default timeout in milliseconds for UI actions
            upload_wait_timeout: Timeout in ms to wait for all files to be queued (default 10 min)
        """
        self._page = page
        self._timeout = timeout
        self._upload_wait_timeout = upload_wait_timeout

    def upload_files(self, folder_path: str) -> None:
        """
        Upload all files from a folder to the current DataLumos project.

        If the folder contains subfolders, zips all contents and uses
        Import From Zip. Otherwise, uses Upload Files and uploads one-by-one.

        Args:
            folder_path: Path to folder containing files to upload

        Raises:
            FileNotFoundError: If folder doesn't exist
            RuntimeError: If upload flow fails
        """
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        if not folder.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {folder_path}")

        use_zip = self._folder_has_subfolders(folder_path)
        if use_zip:
            zip_path = self._zip_folder_contents(folder_path)
            try:
                self._upload_via_import_from_zip(zip_path)
            finally:
                zip_path.unlink(missing_ok=True)
        else:
            files = self.get_file_paths(folder_path)
            if not files:
                Logger.info(f"No files to upload in {folder_path}")
                return
            self._upload_via_upload_files(files)
        Logger.info("File upload completed and modal closed")

    def _folder_has_subfolders(self, folder_path: str) -> bool:
        """Return True if the folder contains any subdirectories."""
        folder = Path(folder_path)
        return any(p.is_dir() for p in folder.iterdir())

    def _zip_folder_contents(self, folder_path: str) -> Path:
        """
        Zip all files and subfolders in the given folder.

        Returns:
            Path to the temporary zip file (caller must delete).
        """
        folder = Path(folder_path)
        fd, zip_path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        zip_path_obj = Path(zip_path)
        base_name = str(zip_path_obj.with_suffix(""))
        shutil.make_archive(base_name, "zip", folder, ".")
        Logger.info(f"Zipped contents of {folder_path} to {zip_path_obj.name}")
        return zip_path_obj

    def _upload_via_import_from_zip(self, zip_path: Path) -> None:
        """Open Import From Zip modal and upload the zip via drop zone."""
        Logger.info(f"Uploading zip via Import From Zip: {zip_path.name}")
        self._wait_for_obscuring_elements()

        import_zip_btn = self._page.locator(IMPORT_FROM_ZIP_BTN_SELECTOR)
        import_zip_btn.click()
        self._page.wait_for_timeout(1000)
        self._wait_for_obscuring_elements()

        self._do_drop_zone_upload([zip_path], use_zip=True)
        self._close_modal(use_zip=True)

    def _upload_via_upload_files(self, files: List[Path]) -> None:
        """Open Upload Files modal and upload files one-by-one via drop zone."""
        Logger.info(f"Uploading {len(files)} file(s) via Upload Files")
        self._wait_for_obscuring_elements()

        upload_btn = self._page.locator(UPLOAD_BTN_SELECTOR)
        upload_btn.click()
        self._page.wait_for_timeout(1000)
        self._wait_for_obscuring_elements()

        self._do_drop_zone_upload(files, use_zip=False)
        self._close_modal(use_zip=False)

    def _do_drop_zone_upload(self, paths: List[Path], use_zip: bool) -> None:
        """Inject file input, upload paths via drop zone, wait for completion."""
        drop_zone_selector = DROP_ZONE_SELECTOR_FROM_ZIP if use_zip else DROP_ZONE_SELECTOR
        drop_zone = self._page.locator(drop_zone_selector)
        drop_zone.wait_for(state="visible", timeout=self._timeout)

        input_id = self._page.evaluate(JS_INJECT_FILE_INPUT, drop_zone_selector)
        if not input_id:
            raise RuntimeError("Could not find drop zone for file upload")

        file_input = self._page.locator(f"#{input_id}")
        try:
            for idx, path in enumerate(paths):
                file_input.set_input_files(str(path))
                if use_zip:
                    upload_btn = self._page.locator("#uploadButton")
                    upload_btn.click()
                    self.wait_for_zip_upload_completion()
                else:
                    self._wait_until_queued_count(use_zip=False, expected=idx + 1)
        except Exception as e:
            self._remove_injected_input()
            raise RuntimeError(f"Error uploading '{paths[0].name if paths else '?'}': {e}") from e
        self._remove_injected_input()

    def _close_modal(self, use_zip: bool) -> None:
        """Close the import/upload modal."""
        close_btn = self._page.locator(CLOSE_MODAL_SELECTOR_FROM_ZIP if use_zip else CLOSE_MODAL_SELECTOR)
        close_btn.click()
        self._wait_for_obscuring_elements()

    def _remove_injected_input(self) -> None:
        """Remove the injected file input from the DOM."""
        try:
            self._page.evaluate(
                f"() => {{ const el = document.getElementById('{INJECTED_INPUT_ID}'); if (el) el.remove(); }}"
            )
        except Exception:
            pass

    def _wait_for_obscuring_elements(self) -> None:
        """Wait for busy overlay to disappear."""
        busy = self._page.locator("#busy")
        try:
            if busy.count() > 0:
                busy.first.wait_for(state="hidden", timeout=360000)
                self._page.wait_for_timeout(500)
        except PlaywrightTimeoutError:
            Logger.warning("Timeout waiting for busy overlay to disappear")

    def get_file_paths(self, folder_path: str) -> List[Path]:
        """
        Get list of file paths to upload from a folder.

        Args:
            folder_path: Path to folder containing files

        Returns:
            List of Path objects for files (not subdirectories)

        Raises:
            FileNotFoundError: If folder doesn't exist
            NotADirectoryError: If path is not a directory
        """
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        if not folder.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {folder_path}")
        files = [f for f in folder.iterdir() if f.is_file()]
        Logger.debug(f"Found {len(files)} files in {folder_path}")
        return files

    def _upload_modal_selector(self, use_zip: bool) -> str:
        return ".importZipModal" if use_zip else ".importFileModal"

    def _queued_completion_signal_count(self, use_zip: bool) -> int:
        """
        Count how many per-file completion signals appear in the modal.

        DataLumos may render status in a <legend>, <span>, or other nodes — not only span.
        Sometimes one status region repeats the phrase; use max(element matches, substring count).
        """
        modal = self._page.locator(self._upload_modal_selector(use_zip))
        if use_zip:
            phrase = FILE_QUEUED_TEXT_FROM_ZIP
            substr = ZIP_STATUS_SUBSTRING
        else:
            phrase = FILE_QUEUED_PHRASE
            substr = FILE_QUEUED_PHRASE
        n_elem = 0
        try:
            n_elem = modal.get_by_text(phrase, exact=False).count()
        except Exception:
            pass
        n_text = 0
        try:
            txt = modal.inner_text(timeout=5000)
            n_text = txt.count(substr)
        except PlaywrightTimeoutError:
            pass
        except Exception:
            pass
        return max(n_elem, n_text)

    def _wait_until_queued_count(self, use_zip: bool, expected: int) -> None:
        if expected <= 0:
            return
        modal_sel = self._upload_modal_selector(use_zip)
        label = FILE_QUEUED_TEXT_FROM_ZIP if use_zip else FILE_QUEUED_PHRASE
        deadline = time.monotonic() + self._upload_wait_timeout / 1000.0
        while time.monotonic() < deadline:
            self._wait_for_obscuring_elements()
            n = self._queued_completion_signal_count(use_zip)
            if n >= expected:
                Logger.debug(
                    f"Upload modal {modal_sel}: {n} completion signal(s) (target {expected})"
                )
                return
            self._page.wait_for_timeout(400)
        raise TimeoutError(
            f"Upload did not reach {expected} completion signal(s) within {self._upload_wait_timeout} ms "
            f"({label!r} in {modal_sel})"
        )

    def wait_for_upload_completion(self, file_count: int) -> None:
        """
        Wait for all file uploads to be queued.

        Looks for "File added to queue for upload" in the import modal (any element,
        including legend), polling until the signal count reaches file_count.

        Args:
            file_count: Number of files being uploaded

        Raises:
            TimeoutError: If uploads don't complete within upload_wait_timeout
        """
        self._wait_until_queued_count(use_zip=False, expected=file_count)
        Logger.debug(f"All {file_count} file(s) added to queue")

    def wait_for_zip_upload_completion(self) -> None:
        """
        Wait for the zip import to show completion / extraction status in the zip modal.

        Raises:
            TimeoutError: If uploads don't complete within upload_wait_timeout
        """
        self._wait_until_queued_count(use_zip=True, expected=1)
        Logger.debug("Zip file(s) added to queue")
