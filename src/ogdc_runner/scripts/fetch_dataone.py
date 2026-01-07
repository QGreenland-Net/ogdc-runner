"""Fetch a DataONE object to a specified directory."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from d1_client.mnclient_2_0 import MemberNodeClient_2_0


def main():
    parser = argparse.ArgumentParser(description="Fetch DataONE object")
    parser.add_argument("--identifier", required=True, help="DataONE PID")
    parser.add_argument("--member-node", required=True, help="Member node URL")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument(
        "--filename", help="Output filename (auto-detect if not provided)"
    )

    args = parser.parse_args()

    try:
        # Initialize client
        client = MemberNodeClient_2_0(base_url=args.member_node)

        # Get system metadata
        sysmeta = client.getSystemMetadata(args.identifier)

        # Determine filename
        if args.filename:
            filename = args.filename
        # Try to get from system metadata
        elif hasattr(sysmeta, "fileName") and sysmeta.fileName:
            filename = sysmeta.fileName
        else:
            # Use identifier
            format_id = str(sysmeta.formatId).lower()
            if "csv" in format_id and not filename.endswith(".csv"):
                filename += ".csv"
            elif "netcdf" in format_id and not filename.endswith(".nc"):
                filename += ".nc"

        # Fetch object
        response = client.get(args.identifier)

        # Write to output
        output_path = Path(args.output_dir) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with Path.open(output_path, "wb") as f:
            f.write(response.content)

        print(
            f"✓ Downloaded {args.identifier} -> {filename} ({len(response.content)} bytes)"
        )
        return 0

    except Exception as e:
        print(f"✗ Failed to fetch {args.identifier}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
