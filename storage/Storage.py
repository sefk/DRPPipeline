"""
Storage singleton for DRP Pipeline.

Provides a centralized storage accessible via direct method calls, similar to Args and Logger.
All StorageProtocol methods are accessible directly on the Storage class.

Example usage:
    from storage import Storage
    
    # Initialize storage using implementation class name
    Storage.initialize('StorageSQLLite', db_path="drp_pipeline.db")
    
    # Use storage methods directly on the class
    drpid = Storage.create_record("https://example.com")
    record = Storage.get(drpid)
    Storage.update_record(drpid, {"title": "My Project", "status": "sourced"})
    Storage.delete(drpid)
    
    # For testing: reset singleton
    Storage.reset()
"""

from pathlib import Path
from typing import Optional

from storage.StorageProtocol import StorageProtocol


class StorageMeta(type):
    """Metaclass to delegate all method calls to the underlying storage instance."""
    
    def __getattr__(cls, name: str):
        """Delegate attribute access to the underlying storage instance."""
        # Don't delegate special/private attributes
        if name.startswith('_') or name.startswith('__'):
            raise AttributeError(f"type object 'Storage' has no attribute '{name}'")
        
        if not cls._initialized or cls._instance is None:
            raise RuntimeError("Storage has not been initialized. Call Storage.initialize() first.")
        
        return getattr(cls._instance, name)


class Storage(metaclass=StorageMeta):
    """
    Storage class providing direct access to all StorageProtocol methods.
    
    Storage is a singleton - call initialize() once, then use methods directly on the class.
    """
    
    # Singleton instance
    _instance: Optional[StorageProtocol] = None
    _initialized: bool = False
    
    # Registry mapping class names to their module paths
    _implementations = {
        'StorageSQLLite': 'storage.StorageSQLLite.StorageSQLLite',
    }
    
    @classmethod
    def initialize(cls, implementation: str, db_path: Optional[Path] = None) -> StorageProtocol:
        """
        Create and initialize a storage implementation (singleton).
        
        If already initialized, returns the existing instance. To reinitialize with
        different parameters, call reset() first.
        
        Args:
            implementation: Name of the implementation class (e.g., 'StorageSQLLite')
            db_path: Optional path to storage file/database
            
        Returns:
            Initialized storage instance conforming to StorageProtocol
            
        Raises:
            ValueError: If implementation name is not recognized
            ImportError: If the implementation class cannot be imported
            RuntimeError: If initialization fails
        """
        if cls._instance is not None:
            return cls._instance
        
        if implementation not in cls._implementations:
            raise ValueError(
                f"Unknown storage implementation: {implementation}. "
                f"Available implementations: {', '.join(cls._implementations.keys())}"
            )
        
        # Dynamically import the implementation class
        module_path = cls._implementations[implementation]
        module_name, class_name = module_path.rsplit('.', 1)
        
        try:
            module = __import__(module_name, fromlist=[class_name])
            implementation_class = getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            raise ImportError(
                f"Failed to import storage implementation '{implementation}': {e}"
            ) from e
        
        # Create instance and initialize it
        instance = implementation_class()
        instance.initialize(db_path=db_path)
        
        # Store as singleton
        cls._instance = instance
        cls._initialized = True
        return instance
    
    @classmethod
    def reset(cls) -> None:
        """
        Reset the singleton instance (for testing).
        
        After calling reset(), initialize() must be called again before using Storage methods.
        """
        cls._instance = None
        cls._initialized = False
