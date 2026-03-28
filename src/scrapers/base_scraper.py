from __future__ import annotations

"""
Abstract base class for all ESN scrapers.

Defines the scraping pipeline contract that every child scraper must implement,
and provides shared utilities like JSON archival and structured logging.
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from supabase import Client

logger: logging.Logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base class that every ESN scraper must extend.

    Child classes are required to implement the three pipeline stages:
        1. ``fetch_data``  – async retrieval of raw data from the source.
        2. ``parse_data``  – transform raw data into a clean structure.
        3. ``upsert_to_db`` – persist the parsed data into Supabase.

    The base class provides:
        * ``save_to_json`` – archive scraped data as JSON in ``data/``.
        * ``run``          – the sole async orchestrator for the full pipeline.
    """

    # Root-level archive directory (relative to project root).
    _DATA_DIR: str = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, "data"
    )

    def __init__(self, name: str = "BaseScraper") -> None:
        self.name: str = name
        self._logger: logging.Logger = logging.getLogger(
            f"{__name__}.{self.name}"
        )

    # ------------------------------------------------------------------
    # Abstract pipeline stages – must be implemented by every child class
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_data(self) -> Any:
        """Fetch raw data from the external source (async).

        Returns:
            The raw data in whatever format the source provides.
        """

    @abstractmethod
    def parse_data(self, raw_data: Any) -> list[dict[str, Any]]:
        """Parse / transform raw data into a list of clean records.

        Args:
            raw_data: The output of ``fetch_data``.

        Returns:
            A list of dictionaries ready for persistence.
        """

    @abstractmethod
    def upsert_to_db(
        self, supabase_client: Client, data: List[Dict[str, Any]]
    ) -> None:
        """Upsert parsed records into Supabase.

        Args:
            supabase_client: An authenticated Supabase client instance.
            data: The list of parsed records to upsert.
        """

    # ------------------------------------------------------------------
    # Concrete helpers
    # ------------------------------------------------------------------

    def save_to_json(self, data: list[dict[str, Any]], filename: str) -> str:
        """Persist *data* as a JSON file inside the ``data/`` archive directory.

        The ``data/`` directory is created automatically if it does not exist.

        Args:
            data: The records to serialise.
            filename: Target filename (e.g. ``"events.json"``).

        Returns:
            The absolute path of the written file.
        """
        os.makedirs(self._DATA_DIR, exist_ok=True)
        filepath: str = os.path.join(self._DATA_DIR, filename)

        self._logger.info("Saving %d records to %s …", len(data), filepath)

        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)

        self._logger.info("Successfully saved JSON archive → %s", filepath)
        return filepath

    # ------------------------------------------------------------------
    # Full pipeline orchestrator
    # ------------------------------------------------------------------

    async def run(
        self,
        supabase_client: Client,
        archive_filename: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Execute the full scraping pipeline.

        1. Fetch raw data (await ``fetch_data``).
        2. Parse it into clean records.
        3. Optionally archive to JSON.
        4. Upsert into Supabase.

        Args:
            supabase_client: An authenticated Supabase client instance.
            archive_filename: If provided, records are saved to ``data/<archive_filename>``
                before upserting.

        Returns:
            The list of parsed records.
        """
        self._logger.info("[%s] Starting fetch …", self.name)
        raw_data: Any = await self.fetch_data()
        self._logger.info("[%s] Fetch complete.", self.name)

        self._logger.info("[%s] Parsing data …", self.name)
        parsed_data: list[dict[str, Any]] = self.parse_data(raw_data)
        self._logger.info("[%s] Parsed %d records.", self.name, len(parsed_data))

        if archive_filename:
            self._logger.info("[%s] Archiving to JSON …", self.name)
            self.save_to_json(parsed_data, archive_filename)

        self._logger.info("[%s] Upserting to Supabase …", self.name)
        self.upsert_to_db(supabase_client, parsed_data)
        self._logger.info("[%s] Pipeline finished successfully.", self.name)

        return parsed_data
