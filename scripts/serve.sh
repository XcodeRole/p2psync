#!/bin/bash
# =============================================================================
# P2PSync Serve Script
#
# Starts serve on a single node.
# By default, uses fast mode (no MD5 computation, uses file paths as keys).
# Pass --checksum to enable MD5 verification mode.
#
# Supports both directory and single file serving:
#   --source /path/to/dir    Serve all files in a directory (recursive)
#   --source /path/to/file   Serve a single file
#
# NOTE: Tracker must be started separately before running this script.
#       This script will verify tracker connectivity before proceeding.
#
# After startup, generates a manifest file that can be used with download.sh.
# The manifest is saved to the dump directory.
#
# Manifest format (tab-separated):
#   <full_source_path>\t<key>\t<size_in_bytes>
#
# Usage:
#   bash serve.sh --source /path/to/your/data/ --tracker-addr http://x.x.x.x:9090
#   bash serve.sh --source /path/to/your/data/model.bin --tracker-addr http://x.x.x.x:9090
#   bash serve.sh --source /path/to/your/data/ --tracker-addr http://x.x.x.x:9090 --checksum
#
# Options:
#   --source        Source directory or file to serve (required)
#   --tracker-addr  Full tracker URL, e.g. http://x.x.x.x:9090 (required)
#   --serve-port    Port for serve (default: 8081)
#   --checksum      Enable MD5 checksum mode (slower startup, verifies file integrity)
#   --p2psync-bin   Path to p2psync binary
#   --dump-dir      Directory to store dump/log files (default: /tmp/p2psync_dump)
#   --node-addr     Node address for serve binding (default: auto-detect)
#   --manifest-out  Where to write manifest file (default: $dump-dir/manifest.txt)
# =============================================================================

set -euo pipefail

# ======================== Parse Arguments ========================
SOURCE=""
SERVE_PORT=8081
P2PSYNC_BIN="p2psync"
DUMP_DIR="/tmp/p2psync_dump"
NODE_ADDR=""
TRACKER_ADDR=""
MANIFEST_OUT=""
CHECKSUM=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --source|--source-dir) SOURCE="$2"; shift 2 ;;
        --serve-port)    SERVE_PORT="$2"; shift 2 ;;
        --p2psync-bin)   P2PSYNC_BIN="$2"; shift 2 ;;
        --dump-dir)      DUMP_DIR="$2"; shift 2 ;;
        --node-addr)     NODE_ADDR="$2"; shift 2 ;;
        --tracker-addr)  TRACKER_ADDR="$2"; shift 2 ;;
        --manifest-out)  MANIFEST_OUT="$2"; shift 2 ;;
        --checksum)      CHECKSUM=true; shift ;;
        *) echo "ERROR: Unknown option: $1"; exit 1 ;;
    esac
done

# ======================== Validate ========================
if [[ -z "$SOURCE" ]]; then echo "ERROR: --source is required"; exit 1; fi
if [[ ! -e "$SOURCE" ]]; then echo "ERROR: $SOURCE does not exist"; exit 1; fi
if [[ ! -d "$SOURCE" ]] && [[ ! -f "$SOURCE" ]]; then echo "ERROR: $SOURCE is neither a file nor a directory"; exit 1; fi
if [[ -z "$TRACKER_ADDR" ]]; then echo "ERROR: --tracker-addr is required"; exit 1; fi

# Detect source type
if [[ -d "$SOURCE" ]]; then
    SOURCE_TYPE="directory"
else
    SOURCE_TYPE="file"
fi

# Node address must be specified by the user.
# hostname -I is unreliable in container environments.
if [[ -z "$NODE_ADDR" ]]; then
    echo "ERROR: --node-addr is required (e.g. --node-addr \$YOUR_NODE_IP in container environment)"
    exit 1
fi

TRACKER_URL="$TRACKER_ADDR"

mkdir -p "$DUMP_DIR"

if [[ -z "$MANIFEST_OUT" ]]; then
    MANIFEST_OUT="${DUMP_DIR}/manifest.txt"
fi

if [[ "$CHECKSUM" == "true" ]]; then
    MODE_LABEL="Checksum Mode (MD5 verification enabled)"
else
    MODE_LABEL="Fast Mode (no checksum)"
fi

echo "============================================"
echo " P2PSync Serve ($MODE_LABEL)"
echo "============================================"
echo " NODE_ADDR:    $NODE_ADDR"
echo " TRACKER_URL:  $TRACKER_URL"
echo " SOURCE:       $SOURCE ($SOURCE_TYPE)"
echo " SERVE_PORT:   $SERVE_PORT"
echo " CHECKSUM:     $CHECKSUM"
echo " DUMP_DIR:     $DUMP_DIR"
echo " MANIFEST_OUT: $MANIFEST_OUT"
echo "============================================"

# ======================== Check Tracker Connectivity ========================
echo "Checking tracker connectivity at $TRACKER_URL ..."
TRACKER_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 "${TRACKER_URL}/peers" 2>/dev/null || echo "000")
if [[ "$TRACKER_HTTP_CODE" != "200" ]]; then
    echo ""
    echo "ERROR: Cannot reach tracker at ${TRACKER_URL}/peers (HTTP code: $TRACKER_HTTP_CODE)"
    echo ""
    echo "Please start the tracker first:"
    echo "  $P2PSYNC_BIN tracker --port <PORT>"
    echo ""
    echo "Then re-run this script with:"
    echo "  --tracker-addr http://<TRACKER_IP>:<PORT>"
    exit 1
fi
echo "Tracker is reachable (HTTP 200)."

# ======================== Start Serve ========================
# Pass the source path (file or directory) as a single --path argument.
# For directories, Rust VFS will recursively scan all files (skipping hidden files).
# For single files, Rust VFS will serve just that file.
DUMP_FILE="${DUMP_DIR}/serve.bin"
LOG_FILE="${DUMP_DIR}/serve.log"

if [[ "$CHECKSUM" == "true" ]]; then
    echo "Starting serve (CHECKSUM MODE) on ${NODE_ADDR}:${SERVE_PORT} ..."
    if [[ "$SOURCE_TYPE" == "directory" ]]; then
        echo "NOTE: Computing MD5 for all files, this may take a while for large datasets..."
    fi
    $P2PSYNC_BIN serve \
        --path "$SOURCE" \
        --address "$NODE_ADDR" \
        --port "$SERVE_PORT" \
        --tracker "$TRACKER_URL" \
        --dump-path "$DUMP_FILE" \
        --manifest-out "$MANIFEST_OUT" > "$LOG_FILE" 2>&1 &
else
    echo "Starting serve (FAST MODE) on ${NODE_ADDR}:${SERVE_PORT} ..."
    $P2PSYNC_BIN serve \
        --path "$SOURCE" \
        --address "$NODE_ADDR" \
        --port "$SERVE_PORT" \
        --tracker "$TRACKER_URL" \
        --dump-path "$DUMP_FILE" \
        --no-checksum \
        --manifest-out "$MANIFEST_OUT" > "$LOG_FILE" 2>&1 &
fi
SERVE_PID=$!
echo "Serve PID=$SERVE_PID"

# ======================== Wait for Serve Ready ========================
echo "Waiting for serve to start..."
for (( retry=1; retry<=600; retry++ )); do
    if grep -q "Listening on" "$LOG_FILE" 2>/dev/null; then
        echo "Serve is ready!"
        break
    fi
    if ! kill -0 "$SERVE_PID" 2>/dev/null; then
        echo "ERROR: Serve process died. Log:"
        cat "$LOG_FILE"
        exit 1
    fi
    if (( retry % 30 == 0 )); then
        echo "Still waiting... (${retry}s elapsed)"
    fi
    sleep 1
done

# ======================== Verify Manifest ========================
# Manifest is generated directly by Rust serve (--manifest-out).
# Format (tab-separated): "<full_path>\t<key>\t<size_in_bytes>" per line (leaf files only).
echo "Checking manifest generated by serve..."

# Wait for manifest file to appear (serve writes it before "Listening on")
for (( retry=1; retry<=30; retry++ )); do
    if [[ -s "$MANIFEST_OUT" ]]; then
        break
    fi
    if ! kill -0 "$SERVE_PID" 2>/dev/null; then
        echo "ERROR: Serve process died before writing manifest. Log:"
        cat "$LOG_FILE"
        exit 1
    fi
    sleep 1
done

if [[ ! -s "$MANIFEST_OUT" ]]; then
    echo "ERROR: Manifest file not generated after 30s"
    exit 1
fi

# Display manifest summary (tab-separated: full_path \t key \t size)
TOTAL_MANIFEST=$(wc -l < "$MANIFEST_OUT")
TOTAL_SIZE=0
while IFS=$'\t' read -r fpath key size; do
    TOTAL_SIZE=$((TOTAL_SIZE + size))
    # Human-readable size for display
    if [[ $size -ge 1073741824 ]]; then
        HR_SIZE=$(awk "BEGIN {printf \"%.2f GB\", $size/1073741824}")
    elif [[ $size -ge 1048576 ]]; then
        HR_SIZE=$(awk "BEGIN {printf \"%.2f MB\", $size/1048576}")
    elif [[ $size -ge 1024 ]]; then
        HR_SIZE=$(awk "BEGIN {printf \"%.2f KB\", $size/1024}")
    else
        HR_SIZE="${size} B"
    fi
    # Show just the filename for display, but full path is in manifest
    DISPLAY_NAME=$(basename "$fpath")
    echo "  $DISPLAY_NAME  SIZE: $HR_SIZE"
done < "$MANIFEST_OUT"

# Total size human-readable
if [[ $TOTAL_SIZE -ge 1099511627776 ]]; then
    HR_TOTAL=$(awk "BEGIN {printf \"%.2f TB\", $TOTAL_SIZE/1099511627776}")
elif [[ $TOTAL_SIZE -ge 1073741824 ]]; then
    HR_TOTAL=$(awk "BEGIN {printf \"%.2f GB\", $TOTAL_SIZE/1073741824}")
else
    HR_TOTAL=$(awk "BEGIN {printf \"%.2f MB\", $TOTAL_SIZE/1048576}")
fi
echo "Total: $TOTAL_MANIFEST files, $HR_TOTAL"

echo ""
echo "============================================"
echo " SERVE READY"
echo "============================================"
echo " Total files: $TOTAL_MANIFEST"
echo " Manifest:    $MANIFEST_OUT"
echo " Tracker URL: $TRACKER_URL"
echo " Checksum:    $CHECKSUM"
echo ""
if [[ "$SOURCE_TYPE" == "file" ]]; then
    # For single file, show --key based download command
    SINGLE_KEY=$(head -1 "$MANIFEST_OUT" | cut -d$'\t' -f2)
    echo " To download this file:"
    echo "   bash download.sh \\"
    echo "     --tracker-addr $TRACKER_URL \\"
    echo "     --key \"$SINGLE_KEY\" \\"
    if [[ "$CHECKSUM" == "true" ]]; then
    echo "     --checksum \\"
    fi
    echo "     --output-dir /data/output"
    echo ""
    echo " Or use manifest:"
elif [[ "$SOURCE_TYPE" == "directory" ]]; then
    # For directory, show --key based download command (downloads entire dir)
    # In fast mode, directory key = full path; in checksum mode, key = md5 of children
    if [[ "$CHECKSUM" == "false" ]]; then
        DIR_KEY="$SOURCE"
    else
        # In checksum mode, the directory key is the MD5 hash.
        # It's the last directory entry in dump_md5 output, but we can't easily extract it here.
        # Instead, suggest using manifest mode for checksum.
        DIR_KEY=""
    fi
    if [[ -n "$DIR_KEY" ]]; then
        echo " To download entire directory (all files at once):"
        echo "   bash download.sh \\"
        echo "     --tracker-addr $TRACKER_URL \\"
        echo "     --key \"$DIR_KEY\" \\"
        if [[ "$CHECKSUM" == "true" ]]; then
        echo "     --checksum \\"
        fi
        echo "     --output-dir /data/output"
        echo ""
        echo " Or use manifest (supports multi-node):"
    fi
fi
echo " To download (single node):"
echo "   bash download.sh \\"
echo "     --tracker-addr $TRACKER_URL \\"
echo "     --manifest $MANIFEST_OUT \\"
if [[ "$CHECKSUM" == "true" ]]; then
echo "     --checksum \\"
fi
echo "     --output-dir /data/output"
echo ""
echo " To download (multi-node, set WORLD_SIZE/NODE_RANK env):"
echo "   WORLD_SIZE=4 NODE_RANK=0 bash download.sh \\"
echo "     --tracker-addr $TRACKER_URL \\"
echo "     --manifest $MANIFEST_OUT \\"
if [[ "$CHECKSUM" == "true" ]]; then
echo "     --checksum \\"
fi
echo "     --output-dir /data/output"
echo "============================================"

# ======================== Keep Running ========================
cleanup() {
    echo "Shutting down..."
    kill "$SERVE_PID" 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

echo ""
echo "Serve is running. Press Ctrl+C to stop."
echo "Serve log: tail -f $LOG_FILE"
sleep infinity
