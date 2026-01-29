"""
Data quality validator using Soda Core.
"""

import logging
from typing import List, Dict, Any

from soda.scan import Scan

logger = logging.getLogger(__name__)


class Validator:
    """
    Data quality validator using Soda Core.

    Executes SodaCL checks defined in data contracts.
    """

    def validate(self, contract: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Validate records against contract validation rules using Soda Core with DuckDB.

        Args:
            contract: Data contract with validation.soda_checks
            **kwargs: Must contain 'tmp_paths' (list of Parquet file paths)

        Returns:
            Dict with validation results:
            {
                'is_valid': bool,
                'checks_passed': int,
                'checks_failed': int,
                'failed_checks': list
            }
        """
        tmp_paths = kwargs.get("tmp_paths", [])

        if not tmp_paths:
            logger.warning("No data paths to validate")
            # If no data, is it valid? Let's assume yes but warn
            return {
                "is_valid": True,
                "checks_passed": 0,
                "checks_failed": 0,
                "failed_checks": [],
            }

        # Extract file paths from the tuples (partition, path, count)
        # Note: The paths from writer.write_bronze() are directories,
        # so we use glob patterns to find all parquet files in each directory
        file_paths = []
        for item in tmp_paths:
            if isinstance(item, (tuple, list)) and len(item) >= 2:
                dir_path = item[1]
                # Use glob pattern if it's a directory
                if not dir_path.endswith(".parquet"):
                    file_paths.append(f"{dir_path}/*.parquet")
                else:
                    file_paths.append(dir_path)
            elif isinstance(item, str):
                # Use glob pattern if it's a directory
                if not item.endswith(".parquet"):
                    file_paths.append(f"{item}/*.parquet")
                else:
                    file_paths.append(item)

        if not file_paths:
            logger.warning("No valid file paths found in tmp_paths")
            return {
                "is_valid": True,
                "checks_passed": 0,
                "checks_failed": 0,
                "failed_checks": [],
            }

        # Get Soda checks from contract
        soda_checks = (
            contract.get("resource", {}).get("validation", {}).get("soda_checks", [])
        )

        if not soda_checks:
            logger.info("No Soda checks defined in contract, skipping validation")
            return {
                "is_valid": True,
                "checks_passed": 0,
                "checks_failed": 0,
                "failed_checks": [],
            }

        logger.info(
            f"Validating {len(file_paths)} files with {len(soda_checks)} Soda checks using DuckDB"
        )

        # Setup DuckDB connection
        import duckdb

        con = duckdb.connect(database=":memory:")

        try:
            # Register Parquet files as a table
            # DuckDB doesn't support prepared parameters in CREATE VIEW statements
            # Instead, we read the parquet files and register the result as a virtual table

            table_name = contract.get("resource", {}).get("name", "data")

            # Read parquet files and create a relation
            # This approach avoids the CREATE VIEW with prepared parameter issue
            relation = con.read_parquet(file_paths)

            # Register the relation as a virtual table that Soda can query
            con.register(table_name, relation)

            # Verify the table was registered and log its schema
            try:
                schema_info = con.execute(f"DESCRIBE {table_name}").fetchall()
                logger.info(
                    f"Registered table '{table_name}' with schema: {schema_info}"
                )

                # Also log a sample of the data
                sample_data = con.execute(
                    f"SELECT * FROM {table_name} LIMIT 3"
                ).fetchall()
                logger.info(f"Sample data from '{table_name}': {sample_data}")
            except Exception as e:
                logger.warning(f"Could not describe table: {e}")

            # Create Soda scan
            scan = Scan()
            scan.set_data_source_name("duckdb")
            scan.add_duckdb_connection(con)

            # Build SodaCL YAML from checks list
            sanitized_checks = self._sanitize_checks(soda_checks)
            checks_yaml = self._build_checks_yaml(table_name, sanitized_checks)
            logger.info(f"Generated SodaCL YAML:\n{checks_yaml}")
            scan.add_sodacl_yaml_str(checks_yaml)

            # Execute scan
            scan.execute()

            # Parse results from scan object directly
            # Soda Core stores check results in scan._checks list (private attribute)
            checks_passed = 0
            checks_failed = 0
            failed_checks = []

            # Check if scan has checks results
            if not hasattr(scan, "_checks") or scan._checks is None:
                logger.warning(
                    "Scan completed but no checks found. Soda may have failed to parse checks."
                )
                raise ValueError("Soda scan failed to execute checks properly")

            for check in scan._checks:
                check_name = check.name if hasattr(check, "name") else str(check)
                check_outcome = getattr(check, "outcome", "unknown")

                logger.debug(f"Check '{check_name}' outcome: {check_outcome}")

                check_outcome = str(check_outcome).lower()
                if check_outcome in ("pass", "passed"):
                    checks_passed += 1
                elif check_outcome in ("fail", "failed"):
                    checks_failed += 1
                    failed_checks.append(
                        {
                            "name": check_name,
                            "outcome": check_outcome,
                            "diagnostic": str(check.diagnostic)
                            if hasattr(check, "diagnostic")
                            else None,
                        }
                    )
                else:
                    # Unexpected outcome (None, "unknown", etc.) - treat as failure
                    logger.error(
                        f"Check '{check_name}' failed with unexpected outcome '{check_outcome}'. "
                        f"This usually indicates an error during Soda check execution."
                    )
                    checks_failed += 1
                    failed_checks.append(
                        {
                            "name": check_name,
                            "outcome": str(check_outcome),
                            "diagnostic": f"Unexpected outcome: {check_outcome}. "
                            + (
                                str(check.diagnostic)
                                if hasattr(check, "diagnostic")
                                else "No diagnostic available"
                            ),
                        }
                    )

            is_valid = checks_failed == 0

            logger.info(
                f"Validation complete: passed={checks_passed}, failed={checks_failed}, valid={is_valid}"
            )

            return {
                "is_valid": is_valid,
                "checks_passed": checks_passed,
                "checks_failed": checks_failed,
                "failed_checks": failed_checks,
            }

        except Exception as e:
            logger.error(f"Validation failed with error: {e}")
            raise e
        finally:
            con.close()

    def _sanitize_checks(self, checks: List) -> List:
        """
        Sanitize checks to work around Soda Core issues with DuckDB views.

        Converts row-level checks to metric checks where possible to avoid
        'AttributeError: NoneType object has no attribute column_name'.

        Args:
            checks: List of raw SodaCL checks (strings or dicts for user-defined metrics)

        Returns:
            List of sanitized checks (strings or dicts)
        """
        import re

        sanitized = []
        for check in checks:
            if isinstance(check, dict):
                sanitized.append(check)
                continue

            if not isinstance(check, str):
                logger.warning(f"Skipping check with unexpected type: {type(check)}")
                continue

            # Skip row_count check as it is a built-in metric
            if "row_count" in check:
                sanitized.append(check)
                continue

            # Check for "col > val" pattern (positive)
            # Example: "price_usd > 0" -> "min(price_usd) > 0"
            if re.match(r"^\s*\w+\s*>\=?\s*\d+(\.\d*)?\s*$", check):
                # Extract parts
                parts = re.split(r"(\s*>\=?\s*)", check, maxsplit=1)
                if len(parts) >= 3:
                    col = parts[0].strip()
                    op = parts[1].strip()
                    val = parts[2].strip()
                    sanitized.append(f"min({col}) {op} {val}")
                    continue

            # Check for "col < val" pattern (negative)
            # Example: "price_usd < 0" -> "max(price_usd) < 0"
            if re.match(r"^\s*\w+\s*<\=?\s*\d+(\.\d*)?\s*$", check):
                # Extract parts
                parts = re.split(r"(\s*<\=?\s*)", check, maxsplit=1)
                if len(parts) >= 3:
                    col = parts[0].strip()
                    op = parts[1].strip()
                    val = parts[2].strip()
                    sanitized.append(f"max({col}) {op} {val}")
                    continue

            sanitized.append(check)

        return sanitized

    def _build_checks_yaml(self, table_name: str, checks: List) -> str:
        """
        Build SodaCL YAML from list of checks.

        Args:
            table_name: Name of the table/dataset
            checks: List of check expressions (strings or dicts for user-defined metrics)

        Returns:
            YAML string for Soda scan
        """
        # Build YAML structure
        yaml_lines = [f"checks for {table_name}:"]

        for check in checks:
            if isinstance(check, dict):
                # The dict structure is like: {'metric_name = threshold': {'metric_name query': 'SELECT ...'}}
                for check_expr, nested_config in check.items():
                    yaml_lines.append(f"  - {check_expr}:")
                    for key, value in nested_config.items():
                        if isinstance(value, str) and "\n" in value:
                            yaml_lines.append(f"      {key}: |")
                            for line in value.split("\n"):
                                yaml_lines.append(f"        {line}")
                        else:
                            yaml_lines.append(f"      {key}: {value}")
            else:
                yaml_lines.append(f"  - {check}")

        return "\n".join(yaml_lines).replace("{table}", table_name)
