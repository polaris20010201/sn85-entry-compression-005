#!/bin/sh
# Competition contract, streaming compression: /bin/sh /app/run.sh INPUT_DIR OUTPUT_DIR
# One Python process encodes every item of the batch (streaming_entry.py). Any item it could not write gets a
# minimal preset-12 AV1 encode here, and the script always exits 0 (a non-zero exit zeroes the whole batch).
set -u
umask 022
[ "$#" -eq 2 ] || { echo "usage: /app/run.sh INPUT_DIR OUTPUT_DIR" >&2; exit 64; }
input_dir=$1
output_dir=$2
app_dir=${CONTENDER_APP_DIR:-/app}
python=${PYTHON:-python3}
PATH=/usr/local/bin:/usr/bin:/bin${PATH:+:$PATH}
PYTHONDONTWRITEBYTECODE=1
SVT_LOG=1
export PATH PYTHONDONTWRITEBYTECODE SVT_LOG
BATCH_BUDGET_SECONDS=${BATCH_BUDGET_SECONDS:-900}

if command -v timeout >/dev/null 2>&1; then
    timeout -k 10 "$((BATCH_BUDGET_SECONDS - 60))" "$python" "$app_dir/streaming_entry.py" "$input_dir" "$output_dir"
else
    "$python" "$app_dir/streaming_entry.py" "$input_dir" "$output_dir"
fi

# safety net: every item without an output gets a whole-clip preset-12 encode at CRF 63 (rules not re-checked)
for input_path in "$input_dir"/*; do
    [ -f "$input_path" ] || continue
    name=${input_path##*/}
    case "$name" in .*) continue ;; esac
    [ -s "$output_dir/$name" ] && continue
    fps=$(ffprobe -v error -select_streams v:0 -show_entries stream=r_frame_rate -of csv=p=0 "$input_path" 2>/dev/null)
    gop=$(echo "$fps" | awk -F/ '{d = ($2 == "" ? 1 : $2); g = int(2 * $1 / d); print (g < 1 ? 1 : g)}')
    ffmpeg -v error -nostdin -y -threads 1 -i "$input_path" -map 0:v:0 -an -sn -dn -fps_mode passthrough \
        -c:v libsvtav1 -pix_fmt yuv420p10le -preset 12 -g "$gop" -svtav1-params "scd=0:irefresh-type=2:lp=1:crf=63" \
        -colorspace bt709 -color_primaries bt709 -color_trc bt709 -color_range tv -map_metadata -1 -write_tmcd 0 \
        -f mp4 "$output_dir/.fallback-$name" \
        && mv "$output_dir/.fallback-$name" "$output_dir/$name" && echo "fallback: $name" >&2
    rm -f "$output_dir/.fallback-$name" "$output_dir"/.partial-* 2>/dev/null
done
exit 0
