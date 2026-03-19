"""
DataLumos form field population.

Handles filling all form fields on the DataLumos project page
including text inputs, WYSIWYG editors, dropdowns, and autocomplete fields.
"""

from typing import List, Optional

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from utils.Logger import Logger


def _is_empty(value: Optional[str]) -> bool:
    """Return True if value is None, empty, or whitespace-only."""
    return not value or value.strip() == ""


class DataLumosFormFiller:
    """
    Fills form fields on the DataLumos project page.
    
    Handles different input types:
    - Text inputs
    - WYSIWYG editors (iframe-based)
    - Dropdown selections
    - Autocomplete/tag inputs (select2)
    """
    
    def __init__(self, page: Page, timeout: int = 2000) -> None:
        """
        Initialize the form filler.
        
        Args:
            page: Playwright Page object
            timeout: Default timeout in milliseconds
        """
        self._page = page
        self._timeout = timeout
    
    def wait_for_obscuring_elements(self) -> None:
        """
        Wait for any loading overlays or busy indicators to disappear.
        
        Looks for elements with id="busy" and waits for them to become hidden.
        """
        busy_locator = self._page.locator("#busy")
        try:
            if busy_locator.count() > 0:
                busy_locator.first.wait_for(state="hidden", timeout=360000)  # 6 min
                self._page.wait_for_timeout(500)
        except PlaywrightTimeoutError:
            Logger.warning("Timeout waiting for busy overlay to disappear")
    
    def expand_all_sections(self) -> None:
        """
        Expand all collapsible sections on the form.
        
        Clicks "Collapse All" then "Expand All" to ensure
        all sections are visible.
        """
        collapse_btn = self._page.locator("#expand-init > span:nth-child(2)")
        self.wait_for_obscuring_elements()
        collapse_btn.click()
        self._page.wait_for_timeout(2000)
        
        expand_btn = self._page.locator("#expand-init > span:nth-child(2)")
        self.wait_for_obscuring_elements()
        expand_btn.click()
        self._page.wait_for_timeout(2000)
    
    def fill_title(self, title: str) -> None:
        """
        Fill the project title field and create the project.
        
        Args:
            title: Project title text
        """
        title_input = self._page.locator("#title")
        title_input.fill(title)
        
        save_btn = self._page.locator(".save-project")
        save_btn.click()
        
        # Element has role="button" (not link) per DataLumos markup
        continue_btn = self._page.get_by_role("button", name="Continue To Project Workspace")
        continue_btn.click()
        
        self.wait_for_obscuring_elements()
        self._page.wait_for_timeout(1000)
    
    def fill_agency(self, agencies: List[str]) -> None:
        """
        Fill the government agency field(s).
        
        Adds agency and office as separate values. Each non-empty value
        opens the add-value modal, selects Organization/Agency tab,
        fills orgName, and saves.
        
        Args:
            agencies: List of agency/office names to add (e.g. [agency, office])
        """
        add_value_selector = (
            "#groupAttr0 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > "
            "a:nth-child(3) > span:nth-child(3)"
        )
        
        for value in agencies:
            if _is_empty(value):
                continue
            
            value = value.strip()
            if value == 'CDC':
                value = 'United States Department of Health and Human Services. Centers for Disease Control and Prevention'
            add_btn = self._page.locator(add_value_selector)
            self.wait_for_obscuring_elements()
            add_btn.click()
            
            # Element: <a href="#org" role="tab">Organization/Agency</a> - use href (role="tab" not "link")
            agency_tab = self._page.locator('a[href="#org"]')
            self.wait_for_obscuring_elements()
            agency_tab.click()
            
            org_field = self._page.locator("#orgName")
            org_field.fill(value)
            self._page.wait_for_timeout(500)
            
            self._dismiss_autocomplete_dropdown()
            
            self.wait_for_obscuring_elements()
            submit_btn = self._page.locator(".save-org")
            submit_btn.click()
    
    def _dismiss_autocomplete_dropdown(self) -> None:
        """Dismiss any open autocomplete dropdown."""
        try:
            label = self._page.locator("label[for='orgName']")
            if label.is_visible(timeout=1000):
                label.click()
        except PlaywrightTimeoutError:
            try:
                label = self._page.locator(
                    "xpath=//label[contains(text(), 'Organization') or contains(text(), 'Agency')]"
                )
                if label.first.is_visible(timeout=1000):
                    label.first.click()
            except PlaywrightTimeoutError:
                try:
                    header = self._page.locator(".modal-header, .modal-title").first
                    if header.is_visible(timeout=1000):
                        header.click()
                except PlaywrightTimeoutError:
                    self._page.keyboard.press("Escape")
        
        self._page.wait_for_timeout(300)
    
    def fill_summary(self, summary: str) -> None:
        """Fill the summary/description field (WYSIWYG). Preserves HTML markup."""
        if _is_empty(summary):
            return
        self._fill_wysiwyg(
            "#edit-dcterms_description_0",
            summary,
            "#groupAttr0 iframe.wysihtml5-sandbox",
            ".glyphicon-ok",
        )
    
    def fill_original_url(self, url: str) -> None:
        """Fill the original distribution URL field."""
        if _is_empty(url):
            return
        self._fill_editable_inline(
            "#edit-imeta_sourceURL_0 > span:nth-child(1) > span:nth-child(2)",
            url,
        )
    
    def fill_keywords(self, keywords: List[str]) -> None:
        """
        Fill the subject terms/keywords field.
        
        Uses select2 autocomplete - types each keyword and selects
        the matching suggestion.
        """
        for keyword in keywords:
            keyword = keyword.strip(" '")
            if len(keyword) <= 2:
                continue
            
            try:
                self.wait_for_obscuring_elements()
                search_field = self._page.locator(".select2-search__field")
                search_field.click()
                search_field.fill(keyword)
                self.wait_for_obscuring_elements()
                
                option = self._page.locator(
                    f"xpath=//li[contains(@class, 'select2-results__option') and text()='{keyword}']"
                )
                self.wait_for_obscuring_elements()
                option.click()
            except PlaywrightTimeoutError as e:
                Logger.warning(f"Could not add keyword '{keyword}': {e}")
    
    def fill_geographic_coverage(self, coverage: str) -> None:
        """Fill the geographic coverage field."""
        if _is_empty(coverage):
            return
        self._fill_editable_inline(
            "#edit-dcterms_location_0 > span:nth-child(1) > span:nth-child(2)",
            coverage,
        )
    
    def fill_time_period(self, start: Optional[str], end: Optional[str]) -> None:
        """Fill the time period fields."""
        if _is_empty(start) and _is_empty(end):
            return
        
        add_btn = self._page.locator(
            "#groupAttr1 > div:nth-child(1) > div:nth-child(3) > div:nth-child(1) > "
            "a:nth-child(3) > span:nth-child(3)"
        )
        add_btn.wait_for(state="visible", timeout=50000)
        self.wait_for_obscuring_elements()
        add_btn.click()
        
        start_input = self._page.locator("#startDate")
        start_input.wait_for(state="visible", timeout=50000)
        self.wait_for_obscuring_elements()
        start_input.fill(start or "")
        
        end_input = self._page.locator("#endDate")
        end_input.wait_for(state="visible", timeout=50000)
        self.wait_for_obscuring_elements()
        end_input.fill(end or "")
        
        save_btn = self._page.locator(".save-dates")
        save_btn.wait_for(state="visible", timeout=50000)
        self.wait_for_obscuring_elements()
        save_btn.click()
        self.wait_for_obscuring_elements()

        # The time period modal (React-managed) intercepts pointer events on subsequent
        # fields if it stays open. Press Escape to dismiss it, then wait for the fade-out
        # animation to complete before returning.
        self._page.keyboard.press("Escape")
        try:
            self._page.locator(".modal.fade.in").wait_for(state="hidden", timeout=10000)
        except PlaywrightTimeoutError:
            Logger.warning("Time period modal still visible after Escape; continuing anyway")
            self._page.wait_for_timeout(1000)

    def fill_data_types(self, data_type: str) -> None:
        """
        Fill the data types field by selecting the checklist option.
        
        After clicking edit, an editable-checklist appears with label+span options.
        Clicks the label (not span) scoped to the checklist for robustness.
        """
        if _is_empty(data_type):
            return
        
        edit_btn = self._page.locator("#disco_kindOfData_0 > span:nth-child(2)")
        self.wait_for_obscuring_elements()
        edit_btn.click()
        self.wait_for_obscuring_elements()
        
        # Double-quote wrapper handles apostrophes; escape any internal double quotes
        safe = data_type.replace('"', '\\"')
        datatype_label = self._page.locator(
            f'.editable-checklist label:has(span:has-text("{safe}"))'
        )
        datatype_label.click()
        
        save_btn = self._page.locator(".editable-submit")
        save_btn.click()
    
    def fill_collection_notes(self, notes: str, download_date: Optional[str] = None) -> None:
        """Fill the collection notes field, optionally appending download date."""
        download_part = f"(Downloaded {download_date})" if download_date else ""
        text = (notes or "").strip()
        if text and download_part:
            combined = f"{text} {download_part}"
        elif download_part:
            combined = download_part
        else:
            return
        self._fill_wysiwyg(
            "#edit-imeta_collectionNotes_0",
            combined,
            "#groupAttr1 iframe.wysihtml5-sandbox",
            ".editable-submit",
        )
    
    def _fill_editable_inline(self, edit_selector: str, value: str) -> None:
        """
        Fill an inline-editable field (click edit, type in input, submit).
        
        Args:
            edit_selector: Selector for the edit button
            value: Value to fill
        """
        edit_btn = self._page.locator(edit_selector)
        edit_btn.wait_for(state="visible", timeout=50000)
        self.wait_for_obscuring_elements()
        edit_btn.click()
        
        url_input = self._page.locator(".editable-input > input:nth-child(1)").first
        url_input.wait_for(state="visible", timeout=50000)
        self.wait_for_obscuring_elements()
        url_input.fill(value)
        url_input.press("Enter")
    
    def _fill_wysiwyg(
        self, edit_selector: str, text: str, frame_selector: str, save_selector: str
    ) -> None:
        """
        Fill a WYSIWYG editor field (iframe-based).

        Sets the iframe body's innerHTML so HTML markup (paragraphs, links,
        bold, etc.) is rendered as rich text rather than literal tags.

        Args:
            edit_selector: Selector for the edit button
            text: HTML or plain text to set in the editor body
            frame_selector: Selector for the editor iframe
            save_selector: Selector for the save button
        """
        edit_btn = self._page.locator(edit_selector)
        self.wait_for_obscuring_elements()
        edit_btn.click()

        frame = self._page.frame_locator(frame_selector)
        body = frame.locator("body")
        body.click()
        self._page.wait_for_timeout(300)

        body.evaluate(
            """(el, html) => {
                el.innerHTML = html;
                el.dispatchEvent(new Event('input', { bubbles: true }));
            }""",
            text,
        )
        self._page.wait_for_timeout(300)
        
        self.wait_for_obscuring_elements()
        save_btn = self._page.locator(save_selector).first
        self.wait_for_obscuring_elements()
        save_btn.click()
