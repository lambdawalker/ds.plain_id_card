import os
from pathlib import Path
from typing import Union, List, Tuple, Dict, Iterator


class PathResolver:
    """
    A smart path resolution utility that maps shorthand aliases (e.g., @DS)
    to physical system paths defined in a ~/.lw_paths configuration file.

    This class supports:
    - Smart caching based on file modification time (mtime).
    - Automatic directory creation by default.
    - Dictionary-like access and iteration.
    - System environment variable fallback.
    """

    def __init__(self, config_name: str = ".lw_paths"):
        self._config_file = Path.home() / config_name
        self._cache: Dict[str, str] = {}
        self._last_mtime: float = 0

    def _load_aliases(self, force_reload: bool = False) -> Dict[str, str]:
        """
        Parses the config file if modified since last read or if force_reload is True.
        Supports # comments, KEY=VALUE pairs, and environment variables within the values.
        """
        if not self._config_file.exists():
            return {}

        try:
            current_mtime = self._config_file.stat().st_mtime
            if force_reload or current_mtime > self._last_mtime:
                new_aliases = {}
                with open(self._config_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            k, v = line.split("=", 1)
                            # Expand vars like $USER or $HOME inside the file strings
                            new_aliases[k.strip()] = os.path.expandvars(v.strip())
                self._cache = new_aliases
                self._last_mtime = current_mtime
        except Exception as e:
            # Silent fail for production robustness, but can be logged if needed
            pass

        return self._cache

    def resolve(self, input_data: Union[str, List[str], Tuple[str, str]],
                make: bool = True, ignore_cache: bool = False) -> Path:
        """
        Resolves an alias to a full Path object.

        Args:
            input_data: Either "@ALIAS/path" or ["ALIAS", "path"].
            make: Automatically create parent directories if they don't exist. Defaults to True.
            ignore_cache: Force a re-read of the .lw_paths file.

        Returns:
            pathlib.Path: The absolute resolved path.

        Raises:
            KeyError: If the alias is not found in config or environment.
        """
        aliases = self._load_aliases(force_reload=ignore_cache)

        # Handle list/tuple: ["DS", "subfolder"] or ("DS", "subfolder")
        if isinstance(input_data, (list, tuple)) and len(input_data) >= 2:
            alias_key, remaining_path = input_data[0], input_data[1]

        # Handle string: "@DS/subfolder"
        elif isinstance(input_data, str) and input_data.startswith("@"):
            parts = input_data[1:].split("/", 1)
            alias_key = parts[0]
            remaining_path = parts[1] if len(parts) > 1 else ""

        # Fallback for standard absolute/relative paths
        else:
            return Path(input_data)

        # Priority: .lw_paths file > System Environment Variables
        root_str = aliases.get(alias_key) or os.environ.get(alias_key)

        if root_str:
            final_path = Path(root_str).expanduser() / remaining_path

            if make:
                # Create the directory structure (parents) for the file/folder
                # exist_ok=True prevents errors if the path already exists
                final_path.parent.mkdir(parents=True, exist_ok=True)

            return final_path

        raise KeyError(f"Alias '@{alias_key}' not found in {self._config_file} or System Environment.")

    def __iter__(self) -> Iterator[str]:
        """Allows iterating over alias keys: for a in resolver: ..."""
        return iter(self._load_aliases())

    def items(self):
        """Returns the alias-to-path mapping items (dict-like)."""
        return self._load_aliases().items()

    def keys(self):
        """Returns all available aliases."""
        return self._load_aliases().keys()

    def __call__(self, input_data: Union[str, List[str], Tuple[str, str]], **kwargs) -> Path:
        """Allows using the object as a function: resolve('@DS/data.csv')"""
        return self.resolve(input_data, **kwargs)

    def __getitem__(self, input_data: Union[str, List[str], Tuple[str, str]]) -> Path:
        """Allows: resolve['DS', 'data.csv'] or resolve['DS']"""
        return self.resolve(input_data)

    def __repr__(self) -> str:
        return f"<PathResolver config={self._config_file} aliases={list(self._cache.keys())}>"


# Create a default instance for easy importing
resolve = PathResolver()
