"""Download supported raw datasets into dataset/raw/.

Usage examples:
  python utils/data_pre_process/download_wanted.py --datasets gowalla
  python utils/data_pre_process/download_wanted.py --datasets foursquare_classic,yelp
  python utils/data_pre_process/download_wanted.py --datasets all

Notes:
  - The script auto-loads REPO_ROOT/.env for tokens (HF/Kaggle).
  - `foursquare_os_places` is gated on Hugging Face and requires accepted terms.
  - `yelp` is distributed on Kaggle and requires Kaggle API credentials.
"""

from __future__ import annotations

import argparse
import gzip
import importlib.util
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = REPO_ROOT / "dataset" / "raw"
DEFAULT_ENV_FILE = REPO_ROOT / ".env"


SUPPORTED_DATASETS = (
    "foursquare_classic",
    "foursquare_os_places",
    "gowalla",
    "yelp",
)


ALIASES = {
    "all": "all",
    "foursquare": "foursquare_classic",
    "foursquare_classic": "foursquare_classic",
    "foursquare-classic": "foursquare_classic",
    "tsmc2014": "foursquare_classic",
    "foursquare_os_places": "foursquare_os_places",
    "foursquare-os-places": "foursquare_os_places",
    "fsq_os_places": "foursquare_os_places",
    "fsq-os-places": "foursquare_os_places",
    "gowalla": "gowalla",
    "yelp": "yelp",
}


def _log(message: str) -> None:
    print(message, flush=True)


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_env_file(env_file: Path) -> None:
    if not env_file.exists():
        return

    # Prefer python-dotenv if available, fallback to lightweight parser.
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(dotenv_path=env_file, override=False)
        _log(f"Loaded env from: {env_file}")
        return
    except ImportError:
        pass

    loaded = 0
    with env_file.open("r", encoding="utf-8") as fin:
        for raw_line in fin:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            value = _strip_quotes(value.strip())
            os.environ.setdefault(key, value)
            loaded += 1
    if loaded:
        _log(f"Loaded env from: {env_file} (built-in parser)")


def _normalize_env_aliases() -> None:
    # Kaggle CLI expects KAGGLE_KEY. Allow KAGGLE_API_TOKEN as alias.
    kaggle_api_token = os.environ.get("KAGGLE_API_TOKEN")
    kaggle_key = os.environ.get("KAGGLE_KEY")
    if kaggle_api_token and not kaggle_key:
        os.environ["KAGGLE_KEY"] = kaggle_api_token
        _log("Mapped env alias: KAGGLE_API_TOKEN -> KAGGLE_KEY")


def _download_to_file(urls: list[str], output_path: Path, timeout: int, force: bool) -> None:
    if output_path.exists() and not force:
        _log(f"  Skip existing file: {output_path.name}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".part")

    last_error: Exception | None = None
    for url in urls:
        _log(f"  Downloading: {url}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "QUAM-Eval/Downloader"})
            with urllib.request.urlopen(req, timeout=timeout) as response, temp_path.open("wb") as fout:
                expected_length_raw = response.headers.get("Content-Length")
                expected_length = int(expected_length_raw) if expected_length_raw and expected_length_raw.isdigit() else None
                written = 0
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    fout.write(chunk)
                    written += len(chunk)

            if expected_length is not None and written != expected_length:
                raise OSError(
                    f"Incomplete download for {output_path.name}: expected {expected_length} bytes, got {written} bytes."
                )

            if output_path.suffix.lower() == ".zip":
                try:
                    with zipfile.ZipFile(temp_path, "r") as zf:
                        zf.infolist()
                except zipfile.BadZipFile as exc:
                    raise OSError(f"Downloaded file is not a valid ZIP archive: {output_path.name}") from exc

            temp_path.replace(output_path)
            _log(f"  Saved to: {output_path}")
            return
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            _log(f"  Failed from this URL: {exc}")
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

    raise RuntimeError(f"All candidate URLs failed for {output_path.name}: {last_error}")


def _extract_zip(zip_path: Path, destination_dir: Path) -> None:
    destination_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(destination_dir)


def _extract_gzip(gzip_path: Path, output_path: Path, force: bool) -> None:
    if output_path.exists() and not force:
        _log(f"  Skip existing extracted file: {output_path.name}")
        return
    with gzip.open(gzip_path, "rb") as fin, output_path.open("wb") as fout:
        shutil.copyfileobj(fin, fout)
    _log(f"  Extracted: {output_path.name}")


def _move_file_to_root_if_found(dataset_dir: Path, file_name: str, force: bool) -> None:
    found = list(dataset_dir.rglob(file_name))
    if not found:
        return
    src = found[0]
    dst = dataset_dir / file_name
    if src.resolve() == dst.resolve():
        return
    if dst.exists():
        if force:
            dst.unlink()
        else:
            return
    shutil.move(str(src), str(dst))


def download_foursquare_classic(args: argparse.Namespace) -> None:
    dataset_dir = RAW_ROOT / "foursquare_classic"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    archive_path = dataset_dir / "dataset_tsmc2014.zip"

    urls = [
        "http://www-public.tem-tsp.eu/~zhang_da/pub/dataset_tsmc2014.zip",
        "http://www-public.it-sudparis.eu/~zhang_da/pub/dataset_tsmc2014.zip",
    ]
    force_download = args.force
    if archive_path.exists() and not force_download:
        try:
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.infolist()
        except zipfile.BadZipFile:
            _log("  Existing archive is corrupted; redownloading...")
            force_download = True
    _download_to_file(urls, archive_path, timeout=args.timeout, force=force_download)

    if args.extract:
        _log("  Extracting archive...")
        _extract_zip(archive_path, dataset_dir)
        _move_file_to_root_if_found(dataset_dir, "dataset_TSMC2014_NYC.txt", args.force)
        _move_file_to_root_if_found(dataset_dir, "dataset_TSMC2014_TKY.txt", args.force)
        _move_file_to_root_if_found(dataset_dir, "dataset_TSMC2014_readme.txt", args.force)

    if not args.keep_archive and archive_path.exists():
        archive_path.unlink()
        _log("  Removed archive after extraction.")


def download_gowalla(args: argparse.Namespace) -> None:
    dataset_dir = RAW_ROOT / "gowalla"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "loc-gowalla_totalCheckins.txt.gz": "https://snap.stanford.edu/data/loc-gowalla_totalCheckins.txt.gz",
        "loc-gowalla_edges.txt.gz": "https://snap.stanford.edu/data/loc-gowalla_edges.txt.gz",
    }

    for file_name, url in files.items():
        gzip_path = dataset_dir / file_name
        _download_to_file([url], gzip_path, timeout=args.timeout, force=args.force)
        if args.extract:
            plain_path = dataset_dir / file_name.removesuffix(".gz")
            _extract_gzip(gzip_path, plain_path, force=args.force)
        if not args.keep_archive and gzip_path.exists():
            gzip_path.unlink()
            _log(f"  Removed archive: {gzip_path.name}")


def download_foursquare_os_places(args: argparse.Namespace) -> None:
    try:
        from huggingface_hub import HfApi, snapshot_download
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required for foursquare_os_places. "
            "Install with: python -m pip install huggingface_hub"
        ) from exc

    token = args.hf_token or os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        raise RuntimeError(
            "HF token is required. Pass --hf-token or set HF_TOKEN/HUGGINGFACEHUB_API_TOKEN."
        )

    repo_id = "foursquare/fsq-os-places"
    dataset_dir = RAW_ROOT / "foursquare_os_places"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    api = HfApi(token=token)
    try:
        all_files = api.list_repo_files(repo_id=repo_id, repo_type="dataset")
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).strip()
        lowered = msg.lower()
        if "10013" in lowered or "forbidden by its access permissions" in lowered:
            raise RuntimeError(
                "Network/socket access to Hugging Face is blocked (WinError 10013). "
                "Please check proxy, firewall, VPN, or corporate network policy."
            ) from exc
        raise RuntimeError(f"Failed to query FSQ repository file list: {msg}") from exc
    dates = sorted(
        {
            path.split("/")[1].split("=")[1]
            for path in all_files
            if path.startswith("release/dt=") and len(path.split("/")) > 2
        }
    )
    if not dates:
        raise RuntimeError("Cannot find release dates from Hugging Face dataset repository.")

    selected_date = dates[-1] if args.fsq_os_date == "latest" else args.fsq_os_date
    if selected_date not in dates:
        raise RuntimeError(
            f"Requested --fsq-os-date={selected_date} not found. Available latest date: {dates[-1]}"
        )

    places_prefix = f"release/dt={selected_date}/places/parquet/"
    categories_prefix = f"release/dt={selected_date}/categories/parquet/"
    deltas_prefix = f"release/dt={selected_date}/deltas/parquet/"

    place_files = [path for path in all_files if path.startswith(places_prefix) and path.endswith(".parquet")]
    category_files = [path for path in all_files if path.startswith(categories_prefix) and path.endswith(".parquet")]
    delta_files = [path for path in all_files if path.startswith(deltas_prefix) and path.endswith(".parquet")]

    if not place_files or not category_files:
        raise RuntimeError(
            "Could not find expected FSQ files on Hugging Face for "
            f"dt={selected_date}. Check token permission and accepted dataset terms."
        )

    selected_files = sorted(place_files + category_files)
    if args.include_deltas and delta_files:
        selected_files.extend(sorted(delta_files))
    selected_files.append("README.md")

    _log(
        f"  Downloading foursquare/fsq-os-places release dt={selected_date} "
        f"(include_deltas={args.include_deltas}, files={len(selected_files)})"
    )
    try:
        snapshot_download(
            repo_id=repo_id,
            repo_type="dataset",
            local_dir=str(dataset_dir),
            token=token,
            allow_patterns=selected_files,
        )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).strip()
        lowered = msg.lower()
        if "gated" in lowered or "403" in lowered or "forbidden" in lowered:
            raise RuntimeError(
                "FSQ dataset is gated. Ensure your HF account accepted the dataset terms and the token has access."
            ) from exc
        if "local cache" in lowered or "cannot find the requested files" in lowered:
            raise RuntimeError(
                "Failed to download FSQ files from Hub (connection or access issue). "
                "Please verify HF_TOKEN, accepted terms, and network connectivity."
            ) from exc
        raise RuntimeError(f"FSQ download failed: {msg}") from exc


def _resolve_kaggle_command(kaggle_cmd: str) -> list[str]:
    if shutil.which(kaggle_cmd):
        return [kaggle_cmd]

    if importlib.util.find_spec("kaggle") is not None:
        return [sys.executable, "-m", "kaggle"]

    raise RuntimeError(
        "Kaggle CLI not found. Install with: python -m pip install kaggle "
        "and ensure Kaggle credentials are configured."
    )


def download_yelp(args: argparse.Namespace) -> None:
    dataset_dir = RAW_ROOT / "yelp"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    kaggle_cmd = _resolve_kaggle_command(args.kaggle_cmd)
    cmd = kaggle_cmd + ["datasets", "download", "-d", args.kaggle_dataset, "-p", str(dataset_dir)]
    if args.extract:
        cmd.append("--unzip")

    _log(f"  Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            "Kaggle download failed. Ensure Kaggle CLI is installed and credentials are configured "
            f"(typically ~/.kaggle/kaggle.json). Details: {stderr}"
        )
    if result.stdout.strip():
        _log(result.stdout.strip())


DOWNLOAD_HANDLERS = {
    "foursquare_classic": download_foursquare_classic,
    "foursquare_os_places": download_foursquare_os_places,
    "gowalla": download_gowalla,
    "yelp": download_yelp,
}


def _normalize_dataset_tokens(tokens: list[str]) -> list[str]:
    normalized: list[str] = []
    for token in tokens:
        for part in token.split(","):
            name = part.strip().lower()
            if not name:
                continue
            if name not in ALIASES:
                raise ValueError(
                    f"Unsupported dataset name: {name}. "
                    f"Supported: {', '.join(SUPPORTED_DATASETS)} or all"
                )
            resolved = ALIASES[name]
            if resolved == "all":
                return list(SUPPORTED_DATASETS)
            normalized.append(resolved)
    # Keep order while deduplicating
    seen: set[str] = set()
    ordered = []
    for item in normalized:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download configured datasets into dataset/raw/<dataset_name>/",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["all"],
        help=(
            "Dataset names (space/comma separated). "
            "Supported: foursquare_classic, foursquare_os_places, gowalla, yelp, all"
        ),
    )
    parser.add_argument("--list", action="store_true", help="List supported datasets and exit.")
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help="Path to .env file used to load tokens/credentials.",
    )
    parser.add_argument("--timeout", type=int, default=180, help="HTTP timeout seconds for direct downloads.")
    parser.add_argument("--force", action="store_true", help="Redownload files even if target exists.")
    parser.add_argument("--keep-archive", action="store_true", help="Keep downloaded .zip/.gz files.")
    parser.add_argument(
        "--no-extract",
        dest="extract",
        action="store_false",
        help="Do not extract downloaded archives.",
    )
    parser.set_defaults(extract=True)

    parser.add_argument(
        "--hf-token",
        default=None,
        help="Hugging Face token for foursquare_os_places (fallback: HF_TOKEN env var).",
    )
    parser.add_argument(
        "--fsq-os-date",
        default="latest",
        help="Release date for foursquare_os_places in YYYY-MM-DD format, or latest.",
    )
    parser.add_argument(
        "--include-deltas",
        action="store_true",
        help="Also download deltas parquet for foursquare_os_places.",
    )

    parser.add_argument(
        "--kaggle-cmd",
        default="kaggle",
        help="Kaggle CLI executable name/path.",
    )
    parser.add_argument(
        "--kaggle-dataset",
        default="yelp-dataset/yelp-dataset",
        help="Kaggle dataset identifier for yelp download.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.list:
        print("\n".join(SUPPORTED_DATASETS))
        return 0

    _load_env_file(Path(args.env_file))
    _normalize_env_aliases()

    try:
        datasets = _normalize_dataset_tokens(args.datasets)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    _log(f"Target raw root: {RAW_ROOT}")

    failures: dict[str, str] = {}
    for dataset_name in datasets:
        _log(f"\n==> Downloading [{dataset_name}]")
        try:
            DOWNLOAD_HANDLERS[dataset_name](args)
            _log(f"==> Completed [{dataset_name}]")
        except Exception as exc:  # noqa: BLE001
            failures[dataset_name] = str(exc)
            _log(f"==> Failed [{dataset_name}]: {exc}")

    if failures:
        _log("\nSummary: completed with failures.")
        for name, reason in failures.items():
            _log(f"- {name}: {reason}")
        return 1

    _log("\nSummary: all requested datasets downloaded successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
