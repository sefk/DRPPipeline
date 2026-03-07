"""
DataLumos uploader module.

Implements ModuleProtocol to upload collected data to DataLumos.
Coordinates browser lifecycle, authentication, form filling, and file uploads.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from storage import Storage
from upload.DataLumosBrowserSession import DataLumosBrowserSession
from utils.Args import Args
from utils.Errors import record_error
from utils.Logger import Logger


class DataLumosUploader:
    """
    Upload module that uploads collected project data to DataLumos.
    
    Implements ModuleProtocol. For each eligible project (status="collected"),
    this module: authenticates, creates project, fills form fields,
    and updates Storage with datalumos_id.
    
    Prerequisites: status="collected" and no errors
    Success status: status="upload"
    """
    
    WORKSPACE_URL = "https://www.datalumos.org/datalumos/workspace"
    
    def __init__(self) -> None:
        """Initialize the DataLumos uploader. Config from Args."""
        self._session = DataLumosBrowserSession()

    def run(self, drpid: int) -> None:
        """
        Run the upload process for a single project.
        
        Implements ModuleProtocol. Gets project from Storage, validates,
        uploads to DataLumos, and updates Storage on success.
        
        Args:
            drpid: The DRPID of the project to upload.
        """
        Logger.info(f"Starting upload for DRPID={drpid}")
        
        project = Storage.get(drpid)
        if project is None:
            record_error(drpid, f"Project with DRPID={drpid} not found in Storage")
            return
        
        errors = self._validate_project(project)
        if errors:
            for error in errors:
                record_error(drpid, error)
            return

        source_url = self._get_field(project, "source_url")
        if source_url:
            page = self._session.ensure_browser()
            from upload.GWDANominator import GWDANominator
            nominator = GWDANominator(page, timeout=Args.upload_timeout)
            success, error = nominator.nominate(source_url)
            if not success:
                record_error(drpid, error or "GWDA nomination failed")
                return

        try:
            datalumos_id = self._upload_project(project, drpid)
            Storage.update_record(drpid, {
                "datalumos_id": datalumos_id,
                "status": "uploaded",
            })
            Logger.info(f"Upload completed for DRPID={drpid}, datalumos_id={datalumos_id}")
        except Exception as e:
            record_error(drpid, f"Upload failed: {e}")
            raise
        finally:
            self._session.close()
    
    def _validate_project(self, project: Dict[str, Any]) -> list[str]:
        """Validate required fields. Returns list of error messages."""
        errors: list[str] = []
        if not self._get_field(project, "title"):
            errors.append("Missing required field: title")
        if not self._get_field(project, "summary"):
            errors.append("Missing required field: summary")
        
        folder = self._get_field(project, "folder_path")
        if folder:
            path = Path(folder)
            if not path.exists():
                errors.append(f"Folder path does not exist: {folder}")
            elif not path.is_dir():
                errors.append(f"Folder path is not a directory: {folder}")
        
        return errors
    
    def _get_field(self, project: Dict[str, Any], key: str) -> str:
        """Get and trim a project field. Returns empty string if missing."""
        return (project.get(key) or "").strip()
    
    def _project_url(self, workspace_id: str) -> str:
        """Build URL for a DataLumos project page."""
        return f"{self.WORKSPACE_URL}?goToLevel=project&goToPath=/datalumos/{workspace_id}#"

    def _upload_project(self, project: Dict[str, Any], drpid: int) -> str:
        """
        Upload a project to DataLumos.

        Navigates to workspace, creates a new project, saves datalumos_id to
        Storage, then fills the form.

        Returns:
            The DataLumos workspace ID.
        """
        page = self._session.ensure_browser()
        self._session.ensure_authenticated()

        from upload.DataLumosFormFiller import DataLumosFormFiller

        form_filler = DataLumosFormFiller(page, timeout=Args.upload_timeout)

        Logger.info("Navigating to DataLumos workspace")
        page.goto(self.WORKSPACE_URL, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=120000)

        from upload.DataLumosAuthenticator import wait_for_human_verification
        wait_for_human_verification(page, timeout=60000)

        new_project_btn = page.locator(".btn > span:nth-child(3)")
        form_filler.wait_for_obscuring_elements()
        new_project_btn.click()

        form_filler.fill_title(self._get_field(project, "title"))

        wait_for_human_verification(page, timeout=60000)

        workspace_id = self._extract_workspace_id(page.url)
        if not workspace_id:
            raise RuntimeError(f"Could not extract workspace ID from URL: {page.url}")

        Logger.info(f"Created project with workspace ID: {workspace_id}")
        Storage.update_record(drpid, {"datalumos_id": workspace_id})

        form_filler.expand_all_sections()
        
        agencies = [f for f in [self._get_field(project, "agency"), self._get_field(project, "office")] if f]
        if agencies:
            form_filler.fill_agency(agencies)
        
        form_filler.fill_summary(self._get_field(project, "summary"))
        form_filler.fill_original_url(self._get_field(project, "source_url"))
        
        keywords_raw = self._get_field(project, "keywords")
        if keywords_raw:
            form_filler.fill_keywords(self._parse_keywords(keywords_raw))
        
        geographic = self._get_field(project, "geographic_coverage")
        if geographic:
            form_filler.fill_geographic_coverage(geographic)
        
        time_start = self._get_field(project, "time_start")
        time_end = self._get_field(project, "time_end")
        if time_start or time_end:
            form_filler.fill_time_period(time_start or None, time_end or None)
        
        data_types = self._get_field(project, "data_types")
        if data_types:
            form_filler.fill_data_types(data_types)
        
        notes = self._get_field(project, "collection_notes")
        download_date = self._get_field(project, "download_date")
        if notes or download_date:
            form_filler.fill_collection_notes(notes, download_date or None)
        
        folder_path = self._get_field(project, "folder_path")
        if folder_path:
            from upload.DataLumosFileUploader import DataLumosFileUploader
            file_uploader = DataLumosFileUploader(page, timeout=Args.upload_timeout)
            file_uploader.upload_files(folder_path)

        return workspace_id
    
    def _extract_workspace_id(self, url: str) -> Optional[str]:
        """Extract workspace ID from DataLumos URL."""
        match = re.search(r"/datalumos/(\d+)", url)
        return match.group(1) if match else None
    
    def _parse_keywords(self, keywords_raw: str) -> List[str]:
        """Parse keywords split by commas or semicolons, removing quotes and brackets."""
        cleaned = keywords_raw.replace("'", "").replace("[", "").replace("]", "").replace('"', "")
        return [p.strip() for p in re.split(r"[,;]+", cleaned) if p.strip()]
