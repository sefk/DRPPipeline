"""
Module protocol interface for DRP Pipeline.

Defines the Module protocol that all pipeline modules must follow.
"""

from typing import Protocol


class ModuleProtocol(Protocol):
    """
    Protocol defining the module API interface.
    
    Any class implementing this protocol must provide a run() method
    that processes a single project identified by DRPID.
    
    Modules can access Storage directly to:
    - Get project data: Storage.get(drpid)
    - Update project: Storage.update_record(drpid, {...})
    - Append warnings/errors: Storage.append_to_field(drpid, "warnings"/"errors", text)
    
    For sourcing module (no prerequisite), the orchestrator passes drpid=-1.
    For other modules, the orchestrator passes the actual DRPID of each eligible project.
    """
    
    def run(self, drpid: int) -> None:
        """
        Run the module to process a single project.
        
        Args:
            drpid: The DRPID of the project to process. Use -1 for sourcing (no specific project).
            
        The module should:
        - Get project data from Storage if needed: Storage.get(drpid)
        - Update status on success: Storage.update_record(drpid, {"status": "module_past_tense"})  # e.g. "sourced", "collected"
        - Append warnings/errors: Storage.append_to_field(drpid, "warnings"/"errors", text)
        - Get num_rows from Args if needed: Args.num_rows
        """
        ...
