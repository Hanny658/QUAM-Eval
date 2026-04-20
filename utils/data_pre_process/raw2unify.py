"""Convert raw POI datasets into unified phase-1 tables.

Usage examples:
  python utils/data_pre_process/raw2unify.py --datasets all
  python utils/data_pre_process/raw2unify.py --datasets foursquare_classic gowalla
  python utils/data_pre_process/raw2unify.py --datasets yelp --files yelp_academic_dataset_business.json yelp_academic_dataset_user.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.data_pre_process.adapters import adapt_fsq_2014, adapt_gowalla, adapt_yelp_2024

DEFAULT_RAW_ROOT = REPO_ROOT / "dataset" / "raw"
DEFAULT_OUT_ROOT = REPO_ROOT / "dataset" / "unified" / "v1"

ADAPTERS = {
    "foursquare_classic": adapt_fsq_2014,
    "gowalla": adapt_gowalla,
    "yelp": adapt_yelp_2024,
}

ALIASES = {
    "all": "all",
    "foursquare": "foursquare_classic",
    "foursquare_classic": "foursquare_classic",
    "foursquare-classic": "foursquare_classic",
    "gowalla": "gowalla",
    "yelp": "yelp",
}

TABLE_NAMES = (
    "users",
    "pois",
    "interactions",
    "categories",
    "poi_category_map",
    "social_edges",
    "source_mapping",
)

SCHEMA_VERSION = "phase1-v1"


def _log(message: str) -> None:
    print(message, flush=True)


def _normalize_datasets(tokens: list[str]) -> list[str]:
    normalized: list[str] = []
    for token in tokens:
        for part in token.split(","):
            value = part.strip().lower()
            if not value:
                continue
            if value not in ALIASES:
                supported = ", ".join(sorted(name for name in ALIASES.keys() if name != "all"))
                raise ValueError(f"Unsupported dataset: {value}. Supported: {supported}, all")
            resolved = ALIASES[value]
            if resolved == "all":
                return list(ADAPTERS.keys())
            normalized.append(resolved)
    seen: set[str] = set()
    ordered: list[str] = []
    for name in normalized:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def _resolve_selected_files(files: list[str], raw_root: Path) -> set[Path]:
    selected: set[Path] = set()
    if not files:
        return selected

    for token in files:
        token_path = Path(token)
        matched: list[Path] = []
        if any(char in token for char in "*?[]"):
            matched.extend(Path.cwd().glob(token))
            matched.extend(raw_root.glob(token))
        elif token_path.exists():
            matched.append(token_path)
        else:
            matched.extend(raw_root.rglob(token))

        for item in matched:
            if item.is_file():
                selected.add(item.resolve())
    return selected


def _selected_files_for_dataset(selected_files: set[Path], dataset_dir: Path) -> set[Path]:
    dataset_resolved = dataset_dir.resolve()
    scoped: set[Path] = set()
    for file_path in selected_files:
        if dataset_resolved in file_path.parents:
            scoped.add(file_path)
    return scoped


def _partition_pairs(table_name: str, row: dict[str, Any]) -> list[tuple[str, str]]:
    dataset = row.get("dataset") or "unknown"
    pairs = [("dataset", str(dataset))]
    if table_name == "interactions":
        ts = row.get("event_time_utc")
        if isinstance(ts, str) and len(ts) >= 7:
            pairs.append(("year", ts[:4]))
            pairs.append(("month", ts[5:7]))
        else:
            pairs.append(("year", "unknown"))
            pairs.append(("month", "unknown"))
    return pairs


def _group_rows_for_write(table_name: str, rows: list[dict[str, Any]]) -> dict[tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    grouped: dict[tuple[tuple[str, str], ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(_partition_pairs(table_name, row))
        grouped[key].append(row)
    return grouped


def _json_default(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value) if value is not None else None


def _write_group_jsonl(
    out_root: Path,
    table_name: str,
    grouped_rows: dict[tuple[tuple[str, str], ...], list[dict[str, Any]]],
    force: bool,
) -> int:
    written = 0
    for partition, rows in grouped_rows.items():
        table_dir = out_root / table_name
        for key, value in partition:
            table_dir = table_dir / f"{key}={value}"
        table_dir.mkdir(parents=True, exist_ok=True)
        output_path = table_dir / "part-00000.jsonl"
        if output_path.exists() and not force:
            raise FileExistsError(
                f"Output already exists: {output_path}. Use --force to overwrite."
            )
        mode = "w"
        with output_path.open(mode, encoding="utf-8") as fout:
            for row in rows:
                fout.write(json.dumps(row, ensure_ascii=False, default=_json_default) + "\n")
                written += 1
    return written


def _write_group_parquet(
    out_root: Path,
    table_name: str,
    grouped_rows: dict[tuple[tuple[str, str], ...], list[dict[str, Any]]],
    force: bool,
) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Parquet output requires pyarrow. Install with: python -m pip install pyarrow"
        ) from exc

    written = 0
    for partition, rows in grouped_rows.items():
        table_dir = out_root / table_name
        for key, value in partition:
            table_dir = table_dir / f"{key}={value}"
        table_dir.mkdir(parents=True, exist_ok=True)
        output_path = table_dir / "part-00000.parquet"
        if output_path.exists() and not force:
            raise FileExistsError(
                f"Output already exists: {output_path}. Use --force to overwrite."
            )
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, output_path)
        written += len(rows)
    return written


def _write_tables(
    out_root: Path,
    dataset_name: str,
    tables: dict[str, list[dict[str, Any]]],
    output_format: str,
    force: bool,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in TABLE_NAMES:
        rows = tables.get(table_name, [])
        if not rows:
            counts[table_name] = 0
            continue
        grouped_rows = _group_rows_for_write(table_name, rows)
        if output_format == "jsonl":
            counts[table_name] = _write_group_jsonl(out_root, table_name, grouped_rows, force=force)
        elif output_format == "parquet":
            counts[table_name] = _write_group_parquet(out_root, table_name, grouped_rows, force=force)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        _log(f"  Wrote {counts[table_name]} rows to {table_name} (dataset={dataset_name})")
    return counts


def _write_metadata(
    out_root: Path,
    run_args: argparse.Namespace,
    selected_datasets: list[str],
    selected_files: set[Path],
    consumed_files: list[str],
    result_counts: dict[str, dict[str, int]],
) -> None:
    metadata_dir = out_root / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    schema_version_path = metadata_dir / "schema_version.json"
    schema_payload = {
        "schema_version": SCHEMA_VERSION,
        "tables": list(TABLE_NAMES),
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    schema_version_path.write_text(json.dumps(schema_payload, indent=2), encoding="utf-8")

    manifest_path = metadata_dir / "build_manifest.json"
    manifest_payload = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "datasets": selected_datasets,
        "selected_files": sorted(str(path) for path in selected_files),
        "consumed_files": sorted(set(consumed_files)),
        "output_format": run_args.format,
        "limit": run_args.limit,
        "counts": result_counts,
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize raw POI datasets to unified phase-1 tables.")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["all"],
        help="Datasets to process (space/comma separated): foursquare_classic, gowalla, yelp, all",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=[],
        help=(
            "Optional file paths, globs, or basenames to process. "
            "When provided, only matching files are processed."
        ),
    )
    parser.add_argument(
        "--strict-files",
        action="store_true",
        help="Fail if any --files entry does not map to a consumed adapter file.",
    )
    parser.add_argument(
        "--raw-root",
        default=str(DEFAULT_RAW_ROOT),
        help="Raw dataset root (default: dataset/raw).",
    )
    parser.add_argument(
        "--out",
        default=str(DEFAULT_OUT_ROOT),
        help="Unified output root (default: dataset/unified/v1).",
    )
    parser.add_argument(
        "--format",
        choices=("jsonl", "parquet"),
        default="jsonl",
        help="Output format.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional max interactions per adapter for smoke tests.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing partition output files.",
    )
    parser.add_argument("--strict", action="store_true", help="Enable strict adapter input checks.")
    parser.add_argument("--list", action="store_true", help="List supported datasets and exit.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.list:
        print("\n".join(ADAPTERS.keys()))
        return 0

    raw_root = Path(args.raw_root).resolve()
    out_root = Path(args.out).resolve()

    try:
        datasets = _normalize_datasets(args.datasets)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    selected_files = _resolve_selected_files(args.files, raw_root)
    if args.files and not selected_files:
        message = "No files resolved from --files input."
        if args.strict_files:
            parser.error(message)
            return 2
        _log(f"Warning: {message}")

    out_root.mkdir(parents=True, exist_ok=True)
    _log(f"Raw root: {raw_root}")
    _log(f"Out root: {out_root}")

    all_consumed_files: list[str] = []
    result_counts: dict[str, dict[str, int]] = {}
    failures: dict[str, str] = {}

    for dataset_name in datasets:
        adapter_module = ADAPTERS[dataset_name]
        dataset_dir = raw_root / dataset_name
        dataset_selected_files = _selected_files_for_dataset(selected_files, dataset_dir)
        if selected_files and not dataset_selected_files:
            _log(f"\n==> Skipping [{dataset_name}] because no selected files belong to this dataset.")
            result_counts[dataset_name] = {table_name: 0 for table_name in TABLE_NAMES}
            continue

        _log(f"\n==> Processing [{dataset_name}]")
        try:
            result = adapter_module.adapt(
                raw_dataset_dir=dataset_dir,
                selected_files=dataset_selected_files if dataset_selected_files else None,
                limit=args.limit,
                strict=args.strict,
            )
            table_counts = _write_tables(
                out_root=out_root,
                dataset_name=dataset_name,
                tables=result.tables,
                output_format=args.format,
                force=args.force,
            )
            result_counts[dataset_name] = table_counts
            all_consumed_files.extend(result.consumed_files)
            _log(f"==> Completed [{dataset_name}]")
        except Exception as exc:  # noqa: BLE001
            failures[dataset_name] = str(exc)
            _log(f"==> Failed [{dataset_name}]: {exc}")

    if args.strict_files and selected_files:
        consumed_resolved = {Path(path).resolve() for path in all_consumed_files}
        consumed_names = {path.name for path in consumed_resolved}
        missing = sorted(
            path
            for path in selected_files
            if path not in consumed_resolved and path.name not in consumed_names
        )
        if missing:
            _log("\nSummary: unresolved selected files:")
            for item in missing:
                _log(f"- {item}")
            return 1

    _write_metadata(
        out_root=out_root,
        run_args=args,
        selected_datasets=datasets,
        selected_files=selected_files,
        consumed_files=all_consumed_files,
        result_counts=result_counts,
    )

    if failures:
        _log("\nSummary: completed with failures.")
        for name, reason in failures.items():
            _log(f"- {name}: {reason}")
        return 1

    _log("\nSummary: unified conversion completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
