"""
Transformer to convert raw JSON to flat records based on extraction rules.
"""

import logging
import hashlib
import json
from typing import List, Dict, Any
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


class Transformer:
    """Transform raw API responses to flat records using extraction rules."""

    def __init__(self, contract: Dict[str, Any]):
        """
        Initialize transformer with a data contract.

        Args:
            contract: Data contract containing extraction rules
        """
        self.contract = contract
        self.extract_rules = contract.get("resource", {}).get("extract", {})

    def transform(
        self, raw_response: Dict[str, Any], **context
    ) -> List[Dict[str, Any]]:
        """
        Transform raw JSON response to list of flat records.

        Args:
            raw_response: Raw API response as dict
            **context: Additional context (e.g., batch_id, source_name)

        Returns:
            List of dicts, one per row
        """
        logger.info("Transforming raw response to flat records")

        # Extract data using rules
        extracted_data = {}
        array_fields = []  # Track fields that have [*] patterns

        for column_name, rule in self.extract_rules.items():
            path = rule.get("path", "")
            value = self._extract_value(raw_response, path, **context)

            # Apply type conversion if specified
            type_hint = rule.get("type")
            if type_hint and value is not None:
                value = self._convert_type(value, type_hint)

            # Check if this is an array field (will need zipping)
            if "[*]" in path:
                array_fields.append(column_name)

            extracted_data[column_name] = value

        # Convert to records
        if array_fields:
            # Zip arrays to create multiple rows
            records = self._zip_arrays(extracted_data, array_fields)
        else:
            # Single row
            records = [extracted_data]

        # Inject metadata columns with __ prefix
        batch_id = context.get("batch_id", str(uuid.uuid4()))
        source_name = context.get(
            "source_name", self.contract.get("source", {}).get("name", "unknown")
        )
        ingested_at = datetime.utcnow().isoformat()
        schema_version = self._compute_schema_version()

        for record in records:
            record["__batch_id"] = batch_id
            record["__source_name"] = source_name
            record["__ingested_at"] = ingested_at
            record["__schema_version"] = schema_version

        logger.info(f"Transformed to {len(records)} records")
        return records

    def _convert_type(self, value: Any, type_hint: str) -> Any:
        """
        Convert value based on type hint.

        Args:
            value: Raw value
            type_hint: Type conversion hint

        Returns:
            Converted value
        """
        if type_hint == "timestamp_ms_to_datetime":
            # Convert timestamp in milliseconds to datetime
            if isinstance(value, list):
                return [datetime.fromtimestamp(v / 1000) if v else None for v in value]
            else:
                return datetime.fromtimestamp(value / 1000) if value else None
        elif type_hint == "timestamp_ms_to_date":
            # Convert timestamp in milliseconds to date
            if isinstance(value, list):
                return [
                    datetime.fromtimestamp(v / 1000).date() if v else None
                    for v in value
                ]
            else:
                return datetime.fromtimestamp(value / 1000).date() if value else None
        elif type_hint == "string":
            return str(value) if value is not None else None
        elif type_hint == "float":
            return float(value) if value is not None else None
        elif type_hint == "integer":
            return int(value) if value is not None else None
        else:
            return value

    def _extract_value(self, data: Any, path: str, **context) -> Any:
        """
        Extract value from nested dict/list using JSONPath-like syntax.

        Supports:
        - Dot notation: "field.nested"
        - Array indexing: "field[0]"
        - Array wildcard: "field[*]" (returns list)
        - Special paths: "_param.key" (extract from context)

        Args:
            data: Data structure to extract from
            path: Path expression
            **context: Context for special paths

        Returns:
            Extracted value
        """
        # Handle special paths
        if path.startswith("_param."):
            param_name = path.split(".", 1)[1]
            return context.get(param_name)

        # Parse path
        current = data
        parts = self._parse_path(path)

        for part in parts:
            if isinstance(part, int):
                # Array index
                current = current[part] if isinstance(current, list) else None
            elif part == "*":
                # Array wildcard - return list
                if isinstance(current, list):
                    # Continue extraction for remaining parts
                    remaining_parts = parts[parts.index(part) + 1 :]
                    if remaining_parts:
                        # Extract from each element
                        return [
                            self._extract_from_parts(item, remaining_parts)
                            for item in current
                        ]
                    else:
                        return current
                else:
                    return []
            else:
                # Dict key
                current = current.get(part) if isinstance(current, dict) else None

            if current is None:
                break

        return current

    def _parse_path(self, path: str) -> List:
        """
        Parse path expression into parts.

        Examples:
            "prices[*][0]" -> ["prices", "*", 0]
            "results[*].t" -> ["results", "*", "t"]

        Args:
            path: Path expression

        Returns:
            List of path parts (strings, ints, or '*')
        """
        parts = []
        current = ""
        i = 0

        while i < len(path):
            char = path[i]

            if char == ".":
                if current:
                    parts.append(current)
                    current = ""
            elif char == "[":
                if current:
                    parts.append(current)
                    current = ""
                # Find closing bracket
                j = path.index("]", i)
                bracket_content = path[i + 1 : j]
                if bracket_content == "*":
                    parts.append("*")
                else:
                    parts.append(int(bracket_content))
                i = j
            else:
                current += char

            i += 1

        if current:
            parts.append(current)

        return parts

    def _extract_from_parts(self, data: Any, parts: List) -> Any:
        """Helper to extract value from remaining parts."""
        current = data
        for part in parts:
            if isinstance(part, int):
                current = current[part] if isinstance(current, list) else None
            elif part == "*":
                # Nested wildcard
                return [
                    self._extract_from_parts(item, parts[parts.index(part) + 1 :])
                    for item in current
                ]
            else:
                current = current.get(part) if isinstance(current, dict) else None

            if current is None:
                break

        return current

    def _zip_arrays(
        self, extracted_data: Dict[str, Any], array_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Zip array fields to create multiple rows.

        For CoinGecko example:
            prices = [[ts1, p1], [ts2, p2]]
            market_caps = [[ts1, mc1], [ts2, mc2]]

        Becomes:
            [
                {data_date: ts1, price: p1, market_cap: mc1},
                {data_date: ts2, price: p2, market_cap: mc2}
            ]

        Args:
            extracted_data: Dict with extracted values
            array_fields: List of field names that are arrays

        Returns:
            List of dicts (one per row)
        """
        # Determine row count from first array field
        first_array = extracted_data[array_fields[0]]
        num_rows = len(first_array) if isinstance(first_array, list) else 1

        records = []
        for i in range(num_rows):
            record = {}
            for field_name, value in extracted_data.items():
                if field_name in array_fields:
                    # Extract i-th element from array
                    record[field_name] = (
                        value[i] if isinstance(value, list) and i < len(value) else None
                    )
                else:
                    # Non-array field: replicate across all rows
                    record[field_name] = value
            records.append(record)

        return records

    def _deduplicate_records(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate records by partition keys, keeping the last entry per partition.

        For CoinGecko data, multiple timestamps within the same day convert to the same
        date. We keep the last occurrence (most recent timestamp) for each date.

        Args:
            records: List of record dicts

        Returns:
            Deduplicated list of records
        """
        # Find partition keys from contract
        partition_keys = []
        for col_name, rule in self.extract_rules.items():
            if rule.get("partition_key"):
                partition_keys.append(col_name)

        if not partition_keys:
            # No partition keys defined, return as-is
            return records

        # Group records by partition key values
        # Using a dict to maintain insertion order (last entry wins)
        unique_records = {}
        for record in records:
            # Build partition tuple as key
            partition_tuple = tuple(record.get(key) for key in partition_keys)
            # Last entry for this partition will overwrite previous ones
            unique_records[partition_tuple] = record

        deduplicated = list(unique_records.values())

        if len(deduplicated) < len(records):
            logger.info(
                f"Deduplicated {len(records)} records to {len(deduplicated)} "
                f"(removed {len(records) - len(deduplicated)} duplicates)"
            )

        return deduplicated

    def _compute_schema_version(self) -> str:
        """
        Compute schema version hash from contract extract rules.

        Per-resource versioning: each resource contract gets a version based on
        the hash of its extract rules (field names + types). This allows us to
        detect when API response structure changes.

        Returns:
            8-character hex hash of schema definition
        """
        # Build deterministic representation of schema
        schema_def = {}
        for field_name, rule in sorted(self.extract_rules.items()):
            schema_def[field_name] = {
                "type": rule.get("type"),
                "partition_key": rule.get("partition_key", False),
            }

        # Compute hash
        schema_json = json.dumps(schema_def, sort_keys=True)
        version_hash = hashlib.md5(schema_json.encode()).hexdigest()[:8]

        return version_hash
