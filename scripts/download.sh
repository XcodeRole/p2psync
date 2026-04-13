#!/bin/bash
# =============================================================================
# P2PSync Unified Download Script
#
# Unified entry point for downloading data from P2P serve nodes.
# Supports both single-node and multi-node (partition) download with full
# resume capability at both directory level and file level.
#
# Modes:
#   Single-node: Default when WORLD_SIZE is not set or equals 1
#   Multi-node:  Automatically enabled when WORLD_SIZE > 1
#                Each node automatically gets its partition of the manifest
#
# Resume (--resume flag):
#   When --resume is specified:
#     - Directory level: completed entries tracked in a GLOBAL progress file,
#       auto-skipped. The same progress file is shared across all nodes, so
#       switching between single-node / multi-node / different node counts
#       (m nodes -> n nodes) is fully supported.
#     - File level: Rust client sends Range header for partially downloaded files
#   Default (no --resume):
#     - Directory level: progress is cleared, all entries re-downloaded
#     - File level: existing files are deleted and re-downloaded from scratch
#
# Manifest format (tab-separated):
#   <full_source_path>\t<key>\t<size_in_bytes>
#
# All control files (manifest partitions, progress, logs) are stored under
# $OUTPUT_DIR/.p2psync/ for easy management and reuse.
#
# Download modes:
#   Manifest mode: Download multiple files listed in a manifest file
#   Key mode:      Download a single file or directory by its key (--key)
#                  When the key points to a directory, all files under it are
#                  downloaded automatically (Rust VFS expands the directory tree).
#                  This is ideal for directories with many small files where
#                  copying a large manifest is inconvenient.
#
# Usage:
#   # Single-node download (manifest mode):
#   bash download.sh \
#     --tracker-addr http://x.x.x.x:9090 \
#     --manifest /path/to/manifest.txt \
#     --output-dir /data/output
#
#   # Single file download (key mode):
#   bash download.sh \
#     --tracker-addr http://x.x.x.x:9090 \
#     --key "/path/to/your/data/model.bin" \
#     --output-dir /data/output
#
#   # Directory download (key mode, all files under the dir):
#   bash download.sh \
#     --tracker-addr http://x.x.x.x:9090 \
#     --key "/path/to/your/data/subdir" \
#     --output-dir /data/output
#
#   # Multi-node download (scheduler sets WORLD_SIZE/NODE_RANK env):
#   # e.g. WORLD_SIZE=4 NODE_RANK=0
#   bash download.sh \
#     --tracker-addr http://x.x.x.x:9090 \
#     --manifest /path/to/manifest.txt \
#     --output-dir /data/output
#
# Options:
#   --tracker-addr  Tracker URL, e.g. http://x.x.x.x:9090 (required)
#   --manifest      Path to manifest file (mutually exclusive with --key)
#   --key           File or directory key to download (mutually exclusive with --manifest)
#   --output-dir    Output directory (required)
#   --world-size    Total number of download nodes (env: $WORLD_SIZE, default: 1)
#   --node-rank     Current node index, 0-based (env: $NODE_RANK, default: 0)
#   --concurrency   Download concurrency per entry (default: 32)
#   --parallel      Number of entries to download in parallel (default: 4)
#   --p2psync-bin   Path to p2psync binary
#   --checksum      Enable MD5 verification (default: disabled)
#   --max-retries   Max retries per entry on failure (default: 3)
#   --resume        Resume partial downloads instead of overwriting (default: overwrite)
# =============================================================================

set -euo pipefail

# ======================== Default Values ========================
TRACKER_ADDR=""
MANIFEST=""
KEYS=()  # Array of keys for single-file download mode
OUTPUT_DIR=""
WORLD_SIZE=1
NODE_RANK=0
CONCURRENCY=32
PARALLEL=16
P2PSYNC_BIN="p2psync"
CHECKSUM=false
MAX_RETRIES=3
RESUME=false

# ======================== Read Environment Variables ========================
# Environment variables are read first, CLI arguments override them below.
if [[ -n "${WORLD_SIZE:-}" ]] && [[ "${WORLD_SIZE}" -ge 1 ]] 2>/dev/null; then
    WORLD_SIZE="${WORLD_SIZE}"
fi
if [[ -n "${NODE_RANK:-}" ]] && [[ "${NODE_RANK}" -ge 0 ]] 2>/dev/null; then
    NODE_RANK="${NODE_RANK}"
fi

# ======================== Parse Arguments ========================
while [[ $# -gt 0 ]]; do
    case $1 in
        --tracker-addr) TRACKER_ADDR="$2"; shift 2 ;;
        --manifest)     MANIFEST="$2"; shift 2 ;;
        --key)          KEYS+=("$2"); shift 2 ;;
        --output-dir)   OUTPUT_DIR="$2"; shift 2 ;;
        --world-size)   WORLD_SIZE="$2"; shift 2 ;;
        --node-rank)    NODE_RANK="$2"; shift 2 ;;
        --concurrency)  CONCURRENCY="$2"; shift 2 ;;
        --parallel)     PARALLEL="$2"; shift 2 ;;
        --p2psync-bin)  P2PSYNC_BIN="$2"; shift 2 ;;
        --checksum)     CHECKSUM=true; shift ;;
        --max-retries)  MAX_RETRIES="$2"; shift 2 ;;
        --resume)       RESUME=true; shift ;;
        *) echo "ERROR: Unknown option: $1"; exit 1 ;;
    esac
done

# ======================== Detect Download Mode ========================
# Two modes: manifest mode (--manifest) and key mode (--key)
# They are mutually exclusive.
DOWNLOAD_MODE=""
if [[ -n "$MANIFEST" ]] && [[ ${#KEYS[@]} -gt 0 ]]; then
    echo "ERROR: --manifest and --key are mutually exclusive. Use one or the other."
    exit 1
elif [[ -n "$MANIFEST" ]]; then
    DOWNLOAD_MODE="manifest"
elif [[ ${#KEYS[@]} -gt 0 ]]; then
    DOWNLOAD_MODE="key"
else
    echo "ERROR: Either --manifest or --key is required"
    exit 1
fi

# ======================== Validate Arguments ========================
if [[ -z "$TRACKER_ADDR" ]]; then echo "ERROR: --tracker-addr is required"; exit 1; fi
if [[ "$DOWNLOAD_MODE" == "manifest" ]] && [[ ! -f "$MANIFEST" ]]; then echo "ERROR: Manifest file not found: $MANIFEST"; exit 1; fi
if [[ -z "$OUTPUT_DIR" ]]; then echo "ERROR: --output-dir is required"; exit 1; fi
if [[ "$NODE_RANK" -ge "$WORLD_SIZE" ]]; then echo "ERROR: --node-rank ($NODE_RANK) must be < --world-size ($WORLD_SIZE)"; exit 1; fi
if [[ "$DOWNLOAD_MODE" == "key" ]] && [[ "$WORLD_SIZE" -gt 1 ]]; then
    echo "ERROR: --key mode does not support multi-node (WORLD_SIZE > 1). Use --manifest for multi-node."
    exit 1
fi

# ======================== Auto-detect Multi-node ========================
# Multi-node mode is automatically enabled when WORLD_SIZE > 1.
MULTI_NODE=false
if [[ "$WORLD_SIZE" -gt 1 ]]; then
    MULTI_NODE=true
    echo "[MULTI-NODE] WORLD_SIZE=$WORLD_SIZE, NODE_RANK=$NODE_RANK"
fi

# ======================== Setup Control Directory ========================
# All control files go under OUTPUT_DIR/.p2psync/
CTRL_DIR="${OUTPUT_DIR}/.p2psync"
mkdir -p "$CTRL_DIR" "$OUTPUT_DIR"

# Progress file: tracks completed entries (one source path per line)
PROGRESS_FILE="${CTRL_DIR}/progress"
PROGRESS_LOCK="${CTRL_DIR}/progress.lock"

# ======================== Progress Functions ========================
is_completed() {
    local KEY="$1"
    grep -qxF "$KEY" "$PROGRESS_FILE" 2>/dev/null
}

mark_completed() {
    local KEY="$1"
    (
        flock -x 200
        echo "$KEY" >> "$PROGRESS_FILE"
    ) 200>"$PROGRESS_LOCK"
}

# ======================== Resume / Overwrite Handling ========================
if [[ "$RESUME" == "true" ]]; then
    echo "[RESUME] Keeping existing progress, will skip completed entries."
    # Migrate legacy per-node progress files into the global progress file.
    # This ensures backward compatibility when upgrading from the old per-node
    # progress scheme to the new unified global progress file.
    LEGACY_NODE_FILES=("${CTRL_DIR}"/progress_node*)
    if [[ -e "${LEGACY_NODE_FILES[0]:-}" ]]; then
        echo "[RESUME] Migrating legacy per-node progress files to global progress..."
        for LEGACY_FILE in "${LEGACY_NODE_FILES[@]}"; do
            # Skip lock files
            [[ "$LEGACY_FILE" == *.lock ]] && continue
            if [[ -f "$LEGACY_FILE" ]]; then
                (
                    flock -x 200
                    # Append only entries not already in the global progress file
                    while IFS= read -r entry; do
                        if ! grep -qxF "$entry" "$PROGRESS_FILE" 2>/dev/null; then
                            echo "$entry" >> "$PROGRESS_FILE"
                        fi
                    done < "$LEGACY_FILE"
                ) 200>"$PROGRESS_LOCK"
            fi
        done
        # Remove legacy files after migration
        rm -f "${CTRL_DIR}"/progress_node*
        echo "[RESUME] Migration complete."
    fi
else
    echo "[OVERWRITE] Clearing progress for a fresh download."
    rm -f "${CTRL_DIR}/progress"
    rm -f "${CTRL_DIR}/progress.lock"
    # Also clean up legacy per-node progress files
    rm -f "${CTRL_DIR}/progress_node"*
    rm -f "${CTRL_DIR}/progress_node"*.lock
fi

# ===========================================================================
# KEY MODE: Download individual files by key (no manifest needed)
# ===========================================================================
if [[ "$DOWNLOAD_MODE" == "key" ]]; then
    # Key mode: NO script-level progress tracking.
    # Rust handles file-level resume (size check + Range header) for each file.
    # This ensures that when a key is a directory and new files are added after
    # serve restart, re-running with --resume will correctly download the new
    # files while skipping already-completed ones.

    TOTAL=${#KEYS[@]}

    echo ""
    echo "============================================"
    echo " P2PSync Download (KEY MODE)"
    echo "============================================"
    echo " TRACKER_ADDR:  $TRACKER_ADDR"
    echo " OUTPUT_DIR:    $OUTPUT_DIR"
    echo " CONCURRENCY:   $CONCURRENCY"
    echo " PARALLEL:      $PARALLEL"
    echo " MAX_RETRIES:   $MAX_RETRIES"
    echo " CHECKSUM:      $CHECKSUM"
    echo " RESUME:        $RESUME"
    echo " P2PSYNC_BIN:   $P2PSYNC_BIN"
    echo "--------------------------------------------"
    echo " Total keys:        $TOTAL"
    echo "--------------------------------------------"
    for KEY in "${KEYS[@]}"; do
        echo "   Key: $KEY"
    done
    echo ""
    echo " NOTE: If a key is a directory, all files under"
    echo "       it will be downloaded automatically."
    echo " NOTE: Resume is handled at file level by Rust."
    echo "       New files added after serve restart will"
    echo "       be picked up automatically."
    echo "============================================"
    echo ""

    # Wait for peers
    echo "Waiting for peers to be available..."
    for (( retry=1; retry<=120; retry++ )); do
        PEER_COUNT=$(curl -s "$TRACKER_ADDR/peers" 2>/dev/null \
            | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('peers',[])))" 2>/dev/null \
            || echo "0")
        if [[ "$PEER_COUNT" -gt 0 ]]; then
            echo "Found $PEER_COUNT peer(s), ready to download."
            break
        fi
        if [[ $retry -eq 120 ]]; then
            echo "ERROR: No peers available after 120 retries"; exit 1
        fi
        echo "No peers yet ($retry/120)..."
        sleep 5
    done
    echo ""

    # Sanitize name for log files
    sanitize_for_log() {
        local name="$1"
        echo "$name" | tr '/' '_' | sed 's/^_*//'
    }

    download_entry() {
        local KEY="$1"
        local SAFE_NAME="$2"
        local LOG_FILE="${CTRL_DIR}/log_${SAFE_NAME}.log"

        local CHECKSUM_FLAG="--no-checksum"
        if [[ "$CHECKSUM" == "true" ]]; then
            CHECKSUM_FLAG=""
        fi

        local RESUME_FLAG=""
        if [[ "$RESUME" == "true" ]]; then
            RESUME_FLAG="--resume"
        fi

        (
            cd "$OUTPUT_DIR"
            $P2PSYNC_BIN download \
                --md5 "$KEY" \
                --concurrency "$CONCURRENCY" \
                --tracker "$TRACKER_ADDR" \
                $CHECKSUM_FLAG \
                $RESUME_FLAG \
                > "$LOG_FILE" 2>&1
        )
        return $?
    }

    # Download entry with retries at the script level (key mode)
    download_key_with_retry() {
        local KEY="$1"
        local SAFE_NAME="$2"
        local LOG_FILE="${CTRL_DIR}/log_${SAFE_NAME}.log"

        for (( attempt=1; attempt<=MAX_RETRIES; attempt++ )); do
            if download_entry "$KEY" "$SAFE_NAME"; then
                return 0
            fi
            if [[ $attempt -lt $MAX_RETRIES ]]; then
                echo "[RETRY] Attempt $attempt/$MAX_RETRIES failed for $KEY, retrying in $((attempt * 5))s..."
                sleep $((attempt * 5))
            fi
        done
        echo "[FAILED] All $MAX_RETRIES attempts failed for $KEY (see ${LOG_FILE})"
        return 1
    }

    DOWNLOAD_START_TS=$(date +%s)
    FAILED=0
    SUCCESS=0
    RUNNING=0
    declare -A KEY_PIDS       # PID -> KEY mapping
    declare -A KEY_PID_NAMES  # PID -> safe name mapping

    # Reap one finished background job (key mode)
    reap_one_key() {
        REAPED_KEY_PID=""
        for pid in "${!KEY_PIDS[@]}"; do
            if ! kill -0 "$pid" 2>/dev/null; then
                REAPED_KEY_PID="$pid"
                return 0
            fi
        done
        # All still running, wait for any one
        if wait -n 2>/dev/null; then
            :
        else
            sleep 1
        fi
        for pid in "${!KEY_PIDS[@]}"; do
            if ! kill -0 "$pid" 2>/dev/null; then
                REAPED_KEY_PID="$pid"
                return 0
            fi
        done
        return 1
    }

    for KEY in "${KEYS[@]}"; do
        SAFE_NAME=$(sanitize_for_log "$KEY")
        ENTRY_IDX=$((SUCCESS + FAILED + RUNNING + 1))
        echo "[${ENTRY_IDX}/${TOTAL}] Downloading: $KEY"

        download_key_with_retry "$KEY" "$SAFE_NAME" &
        KEY_PIDS[$!]="$KEY"
        KEY_PID_NAMES[$!]="$SAFE_NAME"
        RUNNING=$((RUNNING + 1))

        # Throttle parallelism
        if [[ $RUNNING -ge $PARALLEL ]]; then
            if reap_one_key && [[ -n "$REAPED_KEY_PID" ]]; then
                DONE_KEY="${KEY_PIDS[$REAPED_KEY_PID]}"
                DONE_SAFE="${KEY_PID_NAMES[$REAPED_KEY_PID]}"
                if wait "$REAPED_KEY_PID" 2>/dev/null; then
                    SUCCESS=$((SUCCESS + 1))
                    echo "[DONE] Completed: $DONE_KEY"
                else
                    FAILED=$((FAILED + 1))
                    echo "[FAILED] $DONE_KEY (see ${CTRL_DIR}/log_${DONE_SAFE}.log)"
                fi
                unset "KEY_PIDS[$REAPED_KEY_PID]"
                unset "KEY_PID_NAMES[$REAPED_KEY_PID]"
                RUNNING=$((RUNNING - 1))
            fi
        fi
    done

    # Wait for remaining processes
    for PID in "${!KEY_PIDS[@]}"; do
        DONE_KEY="${KEY_PIDS[$PID]}"
        DONE_SAFE="${KEY_PID_NAMES[$PID]}"
        if wait "$PID" 2>/dev/null; then
            SUCCESS=$((SUCCESS + 1))
            echo "[DONE] Completed: $DONE_KEY"
        else
            FAILED=$((FAILED + 1))
            echo "[FAILED] $DONE_KEY (see ${CTRL_DIR}/log_${DONE_SAFE}.log)"
        fi
    done

    DOWNLOAD_END_TS=$(date +%s)
    TOTAL_ELAPSED=$((DOWNLOAD_END_TS - DOWNLOAD_START_TS))
    if [[ $TOTAL_ELAPSED -le 0 ]]; then TOTAL_ELAPSED=1; fi
    ELAPSED_H=$((TOTAL_ELAPSED / 3600))
    ELAPSED_M=$(( (TOTAL_ELAPSED % 3600) / 60 ))
    ELAPSED_S=$((TOTAL_ELAPSED % 60))
    HR_ELAPSED=$(printf "%02d:%02d:%02d" $ELAPSED_H $ELAPSED_M $ELAPSED_S)

    echo ""
    echo "============================================"
    echo " Download Summary (KEY MODE)"
    echo "============================================"
    echo " Total keys:     $TOTAL"
    echo " Succeeded:      $SUCCESS"
    echo " Failed:         $FAILED"
    echo " Total time:     $HR_ELAPSED"
    echo "============================================"

    if [[ $FAILED -gt 0 ]]; then
        echo ""
        echo "WARNING: $FAILED entries failed to download!"
        echo "Re-run with --resume to retry (completed entries will be skipped)."
        echo "Check logs in: ${CTRL_DIR}/log_*.log"
        exit 1
    fi

    echo ""
    echo "All downloads completed successfully!"
    exit 0
fi

# ===========================================================================
# MANIFEST MODE: Download files listed in a manifest file
# ===========================================================================

# Copy manifest to control dir for reference (use basename to avoid path issues)
MANIFEST_BASENAME=$(basename "$MANIFEST")
cp -f "$MANIFEST" "${CTRL_DIR}/${MANIFEST_BASENAME}"

# ======================== Read & Partition Manifest ========================
# Manifest format (tab-separated): <full_path>\t<key>\t<size>
# Read all non-empty lines from manifest
mapfile -t ALL_LINES < <(grep -v '^\s*$' "$MANIFEST")
TOTAL_ALL=${#ALL_LINES[@]}

if [[ $TOTAL_ALL -eq 0 ]]; then
    echo "No entries in manifest. Nothing to download."
    exit 0
fi

if [[ "$MULTI_NODE" == "true" ]]; then
    # ---- Multi-node mode: load-balanced partition (greedy by size) ----
    # Strategy: sort entries by size descending, then greedily assign each entry
    # to the node with the smallest current total. This gives much better balance
    # than round-robin for mixed file sizes (e.g. 900GB + many small files).

    PARTITION_FILE="${CTRL_DIR}/partition_${MANIFEST_BASENAME}_node${NODE_RANK}_of${WORLD_SIZE}.txt"

    # Generate partition using Python for greedy load balancing
    python3 -c "
import sys

lines = []
with open('$MANIFEST') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        # Tab-separated: full_path \t key \t size
        parts = line.split('\t')
        if len(parts) >= 3:
            size = int(parts[2])
        else:
            size = 0
        lines.append((line, size))

# Sort by size descending for greedy assignment
lines.sort(key=lambda x: -x[1])

# Greedy: assign each entry to the node with smallest total
world_size = $WORLD_SIZE
node_totals = [0] * world_size
node_entries = [[] for _ in range(world_size)]

for line, size in lines:
    # Find node with minimum total
    min_node = min(range(world_size), key=lambda n: node_totals[n])
    node_entries[min_node].append(line)
    node_totals[min_node] += size

# Write this node's partition
my_rank = $NODE_RANK
with open('$PARTITION_FILE', 'w') as f:
    for entry in node_entries[my_rank]:
        f.write(entry + '\n')

# Print load balance info
total_size = sum(node_totals)
for i in range(world_size):
    pct = (node_totals[i] / total_size * 100) if total_size > 0 else 0
    marker = ' <-- this node' if i == my_rank else ''
    print(f'  Node {i}: {len(node_entries[i])} entries, {node_totals[i]} bytes ({pct:.1f}%){marker}')
"

    # Read this node's partition
    mapfile -t MY_LINES < <(grep -v '^\s*$' "$PARTITION_FILE")

    # All nodes share the same global progress file.
    # flock ensures concurrent writes are safe across nodes on shared filesystem.
    # This allows seamless resume when switching between different node counts
    # (e.g., 1 node -> 4 nodes, 4 nodes -> 2 nodes, etc.)

    MODE_STR="MULTI-NODE (node ${NODE_RANK}/${WORLD_SIZE})"
else
    # ---- Single-node mode ----
    MY_LINES=("${ALL_LINES[@]}")
    MODE_STR="SINGLE-NODE"
fi

# Create progress file if it doesn't exist
touch "$PROGRESS_FILE"

TOTAL=${#MY_LINES[@]}

# ======================== Count Already Done ========================
# Manifest format (tab-separated): full_path \t key \t size
# KEY is the 2nd field (full source path in no-checksum mode), used as progress key
ALREADY_DONE=0
TOTAL_SIZE=0
for line in "${MY_LINES[@]}"; do
    KEY=$(printf '%s' "$line" | cut -d$'\t' -f2)
    SIZE=$(printf '%s' "$line" | cut -d$'\t' -f3)
    TOTAL_SIZE=$((TOTAL_SIZE + SIZE))
    if is_completed "$KEY"; then
        ALREADY_DONE=$((ALREADY_DONE + 1))
    fi
done

REMAINING=$((TOTAL - ALREADY_DONE))

# Human-readable total size
if [[ $TOTAL_SIZE -ge 1099511627776 ]]; then
    HR_TOTAL=$(awk "BEGIN {printf \"%.2f TB\", $TOTAL_SIZE/1099511627776}")
elif [[ $TOTAL_SIZE -ge 1073741824 ]]; then
    HR_TOTAL=$(awk "BEGIN {printf \"%.2f GB\", $TOTAL_SIZE/1073741824}")
else
    HR_TOTAL=$(awk "BEGIN {printf \"%.2f MB\", $TOTAL_SIZE/1048576}")
fi

# ======================== Print Summary ========================
echo ""
echo "============================================"
echo " P2PSync Download (MANIFEST MODE)"
echo " Mode: ${MODE_STR}"
echo "============================================"
echo " TRACKER_ADDR:  $TRACKER_ADDR"
echo " MANIFEST:     $MANIFEST"
echo " MANIFEST_ID:  ${MANIFEST_BASENAME}"
echo " OUTPUT_DIR:   $OUTPUT_DIR"
echo " CTRL_DIR:     $CTRL_DIR"
echo " CONCURRENCY:  $CONCURRENCY (per entry)"
echo " PARALLEL:     $PARALLEL (entries at once)"
echo " MAX_RETRIES:  $MAX_RETRIES"
echo " CHECKSUM:     $CHECKSUM"
echo " RESUME:       $RESUME"
echo " P2PSYNC_BIN:  $P2PSYNC_BIN"
echo "--------------------------------------------"
echo " Total (manifest):     $TOTAL_ALL"
echo " This node:            $TOTAL ($HR_TOTAL)"
echo " Already completed:    $ALREADY_DONE"
echo " Remaining:            $REMAINING"
echo "--------------------------------------------"
echo " Progress file:  $PROGRESS_FILE"
if [[ $WORLD_SIZE -gt 1 ]]; then
echo " Partition file: $PARTITION_FILE"
fi
echo "============================================"
echo ""

if [[ $REMAINING -eq 0 ]]; then
    echo "All $TOTAL entries already completed! Nothing to download."
    if [[ "$RESUME" == "true" ]]; then
        echo "All entries were already completed from a previous run."
    fi
    exit 0
fi

# ======================== Wait for Peers ========================
echo "Waiting for peers to be available..."
for (( retry=1; retry<=120; retry++ )); do
    PEER_COUNT=$(curl -s "$TRACKER_ADDR/peers" 2>/dev/null \
        | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('peers',[])))" 2>/dev/null \
        || echo "0")
    if [[ "$PEER_COUNT" -gt 0 ]]; then
        echo "Found $PEER_COUNT peer(s), ready to download."
        break
    fi
    if [[ $retry -eq 120 ]]; then
        echo "ERROR: No peers available after 120 retries"; exit 1
    fi
    echo "No peers yet ($retry/120)..."
    sleep 5
done

echo ""

# ======================== Sanitize name for log files ========================
# Replace path separators and special chars with underscores for safe log filenames
sanitize_for_log() {
    local name="$1"
    # Replace / with _ , remove leading underscores
    echo "$name" | tr '/' '_' | sed 's/^_*//'
}

# ======================== Download Logic ========================
FAILED=0
SUCCESS=0
SKIPPED=0
RUNNING=0
declare -A PIDS       # PID -> KEY mapping
declare -A PID_NAMES  # PID -> display name mapping

download_entry() {
    local KEY="$1"
    local SAFE_NAME="$2"
    local LOG_FILE="${CTRL_DIR}/log_${SAFE_NAME}.log"

    local CHECKSUM_FLAG="--no-checksum"
    if [[ "$CHECKSUM" == "true" ]]; then
        CHECKSUM_FLAG=""
    fi

    local RESUME_FLAG=""
    if [[ "$RESUME" == "true" ]]; then
        RESUME_FLAG="--resume"
    fi

    # All output goes to log file to avoid terminal garbling from parallel processes
    (
        cd "$OUTPUT_DIR"
        $P2PSYNC_BIN download \
            --md5 "$KEY" \
            --concurrency "$CONCURRENCY" \
            --tracker "$TRACKER_ADDR" \
            $CHECKSUM_FLAG \
            $RESUME_FLAG \
            > "$LOG_FILE" 2>&1
    )
    return $?
}

# Download entry with retries at the script level
download_entry_with_retry() {
    local KEY="$1"
    local SAFE_NAME="$2"
    local LOG_FILE="${CTRL_DIR}/log_${SAFE_NAME}.log"

    for (( attempt=1; attempt<=MAX_RETRIES; attempt++ )); do
        if download_entry "$KEY" "$SAFE_NAME"; then
            return 0
        fi
        if [[ $attempt -lt $MAX_RETRIES ]]; then
            echo "[RETRY] Attempt $attempt/$MAX_RETRIES failed for $KEY, retrying in $((attempt * 5))s..."
            sleep $((attempt * 5))
        fi
    done
    echo "[FAILED] All $MAX_RETRIES attempts failed for $KEY (see ${LOG_FILE})"
    return 1
}

# Reap one finished background job. Compatible with bash 4.x+.
# Sets REAPED_PID to the PID of the finished job.
reap_one() {
    REAPED_PID=""
    for pid in "${!PIDS[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            REAPED_PID="$pid"
            return 0
        fi
    done
    # All still running, wait for any one
    # Use 'wait -n' if available (bash 4.3+), otherwise poll
    if wait -n 2>/dev/null; then
        : # Some job finished
    else
        sleep 1
    fi
    for pid in "${!PIDS[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            REAPED_PID="$pid"
            return 0
        fi
    done
    return 1
}

DOWNLOAD_START_TS=$(date +%s)

# Download entries in parallel batches
# Manifest format (tab-separated): full_path \t key \t size
for line in "${MY_LINES[@]}"; do
    FPATH=$(printf '%s' "$line" | cut -d$'\t' -f1)
    KEY=$(printf '%s' "$line" | cut -d$'\t' -f2)

    # Skip empty lines
    [[ -z "$KEY" ]] && continue

    # Skip already completed entries (keyed by full source path)
    if is_completed "$KEY"; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Create a safe name for log files from the key
    SAFE_NAME=$(sanitize_for_log "$KEY")

    ENTRY_IDX=$((SKIPPED + SUCCESS + FAILED + RUNNING + 1))
    echo "[${ENTRY_IDX}/${TOTAL}] Downloading: $KEY"
    download_entry_with_retry "$KEY" "$SAFE_NAME" &
    PIDS[$!]="$KEY"
    PID_NAMES[$!]="$SAFE_NAME"
    RUNNING=$((RUNNING + 1))

    # Throttle parallelism
    if [[ $RUNNING -ge $PARALLEL ]]; then
        if reap_one && [[ -n "$REAPED_PID" ]]; then
            DONE_KEY="${PIDS[$REAPED_PID]:-unknown}"
            DONE_SAFE="${PID_NAMES[$REAPED_PID]:-unknown}"
            if wait "$REAPED_PID" 2>/dev/null; then
                SUCCESS=$((SUCCESS + 1))
                mark_completed "$DONE_KEY"
                COMPLETED_NOW=$((SUCCESS + FAILED + SKIPPED))
                echo ""
                echo "[DONE ${COMPLETED_NOW}/${TOTAL}] Completed: $DONE_KEY"
            else
                FAILED=$((FAILED + 1))
                COMPLETED_NOW=$((SUCCESS + FAILED + SKIPPED))
                echo ""
                echo "[FAIL ${COMPLETED_NOW}/${TOTAL}] FAILED: $DONE_KEY (see ${CTRL_DIR}/log_${DONE_SAFE}.log)"
            fi
            unset "PIDS[$REAPED_PID]"
            unset "PID_NAMES[$REAPED_PID]"
            RUNNING=$((RUNNING - 1))
        fi
    fi
done

# Wait for remaining processes
for PID in "${!PIDS[@]}"; do
    DONE_KEY="${PIDS[$PID]}"
    DONE_SAFE="${PID_NAMES[$PID]}"
    if wait "$PID" 2>/dev/null; then
        SUCCESS=$((SUCCESS + 1))
        mark_completed "$DONE_KEY"
        COMPLETED_NOW=$((SUCCESS + FAILED + SKIPPED))
        echo ""
        echo "[DONE ${COMPLETED_NOW}/${TOTAL}] Completed: $DONE_KEY"
    else
        FAILED=$((FAILED + 1))
        COMPLETED_NOW=$((SUCCESS + FAILED + SKIPPED))
        echo ""
        echo "[FAIL ${COMPLETED_NOW}/${TOTAL}] FAILED: $DONE_KEY (see ${CTRL_DIR}/log_${DONE_SAFE}.log)"
    fi
done

# ======================== Final Summary ========================
COMPLETED_TOTAL=$(wc -l < "$PROGRESS_FILE" | tr -d ' ')
DOWNLOAD_END_TS=$(date +%s)
TOTAL_ELAPSED=$((DOWNLOAD_END_TS - DOWNLOAD_START_TS))
if [[ $TOTAL_ELAPSED -le 0 ]]; then TOTAL_ELAPSED=1; fi
TOTAL_AVG_SPEED=$(awk "BEGIN {printf \"%.2f\", $TOTAL_SIZE/$TOTAL_ELAPSED/1048576}")
ELAPSED_H=$((TOTAL_ELAPSED / 3600))
ELAPSED_M=$(( (TOTAL_ELAPSED % 3600) / 60 ))
ELAPSED_S=$((TOTAL_ELAPSED % 60))
HR_ELAPSED=$(printf "%02d:%02d:%02d" $ELAPSED_H $ELAPSED_M $ELAPSED_S)

echo ""
echo "============================================"
echo " Download Summary"
if [[ $WORLD_SIZE -gt 1 ]]; then
echo " Node:           ${NODE_RANK}/${WORLD_SIZE}"
fi
echo "============================================"
echo " Total (manifest):  $TOTAL_ALL"
echo " This node:         $TOTAL"
echo " Skipped (done):    $SKIPPED"
echo " Downloaded:        $SUCCESS"
echo " Failed:            $FAILED"
echo " Progress:          $COMPLETED_TOTAL / $TOTAL completed"
echo " Total time:        $HR_ELAPSED"
echo " Avg speed:         ${TOTAL_AVG_SPEED} MB/s"
echo "============================================"

if [[ $FAILED -gt 0 ]]; then
    echo ""
    echo "WARNING: $FAILED entries failed to download!"
    echo "Re-run with --resume to retry (completed entries will be skipped)."
    echo "Check logs in: ${CTRL_DIR}/log_*.log"
    exit 1
fi

echo ""
echo "All downloads completed successfully!"
