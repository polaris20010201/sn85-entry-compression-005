"""sn85-compression-005 streaming-compression entry: SVT-AV1 per 2 s segment with per-segment rate allocation.

usage: python3 streaming_entry.py INPUT_DIR OUTPUT_DIR

Every regular, non-hidden file in INPUT_DIR is one item (a lossless FFV1 clip named by its sha256). Each
item gets one MP4 (AV1, 10-bit) of the same name in OUTPUT_DIR that keeps the manifest's streaming rules:
average <= budget, every 2 s window <= 1.5x budget, keyframe at least every 2 s.

How an item is encoded:
- The clip is cut into closed-GOP segments of int(2*fps) frames. FFV1 is intra-only, so each segment job
  seeks straight to its first frame and every segment starts on the keyframe the rules need.
- Segments are encoded independently, in parallel, and joined without re-encoding.
- Two cheap probe passes (preset 12) learn each segment's size-vs-CRF slope. Up to MAX_PASSES preset-6
  passes then solve one common fractional CRF that fills AIM of the budget. A segment whose own size
  would break the peak cap gets a higher CRF of its own.
- Every pass is joined and measured on its packets, exactly as the validator measures them, and the
  passing file that uses the most budget is kept. Probe files are the fallback.

Threads: the sandbox may cap the process count (`--pids-limit`, threads included). Under a cap, many
single-threaded encoders run side by side (an lp=1 encode needs ~8 threads); without one, wider
encoders run.

Time: no new refinement pass starts after SOFT_SECONDS. Items still without a passing file then get
an emergency preset-12 encode, which is also checked.

Leftover time: every item's output is written as soon as its main search ends. If the batch then
still has time before the speed knee (frames x KNEE_SPF from the start; within it the speed score is
already full), the hardest items (highest CRF) are re-encoded at preset 4, then 3, aiming at the
same budget use (one correction pass if the first misses). An output is only replaced when the slower encode passes
every rule and uses at least the old budget share minus FILL_TOL.
"""
import json
import math
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction

# the competition manifest's streaming_rules (sn85-compression-005)
BPP, KEYINT_S, PEAK_RATIO, PEAK_WINDOW_S = 0.05, 2.0, 1.5, 2.0
# encoder and rate control (tuned on practice-pack-005, which is CC0)
PRESET, PROBE_PRESET, SVT_EXTRA = 6, 12, "enable-qm=1:qm-min=0"
# The validator allows an average of budget x 1.01 and a 2 s peak of budget x 1.5. measure() reads the same packets
# the same way (official scores matched it to 3 decimals), so half of the 1 % slack is kept as margin.
AIM, DONE, MAX_PASSES = 0.99, 0.95, 3
# Under a process cap (lp=1 encoders) a batch takes about passes x one single-threaded segment encode, so the
# capped profile may stop the search earlier (ENTRY_CAPPED_* override it for tests; the evaluator sets no env).
# measured on 24 L40S-server cores, pack official Q unchanged (0.7581) while the run went 81 s -> 68 s
CAPPED_MAX_PASSES = int(os.environ.get("ENTRY_CAPPED_MAX_PASSES", "2"))
CAPPED_DONE = float(os.environ.get("ENTRY_CAPPED_DONE", str(DONE)))
# capped: a pass lasts as long as the longest single-threaded segment encode (a 2 s segment of 50 fps 1080p is
# 100 frames, ~22 s at preset 6), so segments may be cut shorter; each still starts on a keyframe, and the
# rule only bounds the gap from above. 0 = segments of a full 2 s GOP.
CAPPED_SEG_FRAMES = int(os.environ.get("ENTRY_CAPPED_SEG_FRAMES", "60"))
AVG_OK, PEAK_OK, PEAK_AIM = 1.005, 0.99 * PEAK_RATIO, 0.96 * PEAK_RATIO
CRF_MIN, CRF_MAX = 1.0, 63.0
SOFT_SECONDS = float(os.environ.get("ENTRY_SOFT_SECONDS", "150"))
# leftover time: run.sh time up to frames x KNEE_SPF earns the full speed score (speed_fast_spf = 1/30 s per
# frame), so time left there is spent re-encoding the hardest items at slower presets. Measured with the
# lp=1 segment encoder: time relative to preset 6.
KNEE_SPF = float(os.environ.get("ENTRY_KNEE_SPF", str(1 / 30)))
POLISH_PRESETS, POLISH_MARGIN_S, FILL_TOL = (4, 3), 3.0, 0.02
TIME_VS_P6 = {6: 1.0, 4: 2.0, 3: 2.8}
TAGS = ["-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709", "-color_range", "tv"]
# no source metadata and no timecode track: a TIMECODE tag makes the MP4 muxer add a tmcd data track, and the
# concat demuxer then times later segments by that track, pushing a keyframe gap past 2 s (seen on 29.97 fps)
MP4 = ["-map_metadata", "-1", "-write_tmcd", "0", "-f", "mp4"]
ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin", "SVT_LOG": "1"}
START = time.monotonic()


def log(msg: str) -> None:
    print(f"[{time.monotonic() - START:6.1f}s] {msg}", file=sys.stderr, flush=True)


def run(argv: list[str], timeout: float = 900) -> str:
    return subprocess.run(argv, capture_output=True, text=True, check=True, timeout=timeout, env=ENV).stdout


def thread_plan() -> tuple[int, int]:
    """(parallel encoders, SVT lp) that fit the container's process/thread cap and its CPUs."""
    cpus = len(os.sched_getaffinity(0))
    cap = None
    for f in ("/sys/fs/cgroup/pids.max", "/sys/fs/cgroup/pids/pids.max"):
        try:
            v = pathlib.Path(f).read_text().strip()
            cap = None if v == "max" else int(v)
            break
        except (OSError, ValueError):
            continue
    wide = max(1, cpus // 2)  # lp=2 encoders need ~60 threads each
    if cap is None or cap * 0.75 >= wide * 60:
        return wide, 2
    # a tight cap (the Docker runner's --pids-limit=256): ~8 threads per single-threaded encoder
    return max(1, min(cpus, int(cap * 0.75) // 8)), 1


WORKERS, LP = thread_plan()
POOL = ThreadPoolExecutor(WORKERS)


def probe(path: str) -> tuple[int, int, Fraction, int]:
    s = json.loads(run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-count_packets", "-show_entries",
                        "stream=width,height,r_frame_rate,nb_read_packets", "-of", "json", path]))["streams"][0]
    return int(s["width"]), int(s["height"]), Fraction(s["r_frame_rate"]), int(s["nb_read_packets"])


def measure(path: str, fps: float, frames: int) -> tuple[float, float, float, bool, int, bool]:
    """avg kbps, peak kbps, longest keyframe gap, first-is-key, packets (as vidaio/scoring/streaming.py), and
    whether every frame sits within half a frame of its slot on the constant-rate grid (the validator's CFR
    canonicalization otherwise adds or drops frames and refuses the item)."""
    doc = json.loads(run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                          "packet=size,pts_time,flags", "-of", "json", path]))
    pk = [(int(p["size"]), float(p["pts_time"]), "K" in p.get("flags", "")) for p in doc["packets"]]
    sizes = [s for s, _, _ in pk]
    avg = 8 * sum(sizes) / (frames / fps) / 1000
    w = max(1, round(PEAK_WINDOW_S * fps))
    if len(sizes) <= w:
        peak = 8 * sum(sizes) / (len(sizes) / fps) / 1000
    else:
        cur = best = sum(sizes[:w])
        for i in range(w, len(sizes)):
            cur += sizes[i] - sizes[i - w]
            best = max(best, cur)
        peak = 8 * best / (w / fps) / 1000
    pts = [t for _, t, _ in pk]
    start, end = min(pts), max(pts) + 1 / fps
    keys = sorted(t for _, t, k in pk if k)
    first = bool(keys) and abs(keys[0] - start) < 0.5 / fps
    gap = max([b - a for a, b in zip(keys, keys[1:])] + [end - keys[-1], keys[0] - start]) if keys else end - start
    grid = all(abs(t - start - i / fps) < 0.5 / fps for i, t in enumerate(sorted(pts)))
    return avg, peak, gap, first, len(sizes), grid


def encode_segment(src: str, start: int, count: int, rate: Fraction, gop: int, crf: float, preset: int, dst: str) -> int:
    # seek half a frame early: input timestamps are ms-rounded, every FFV1 frame is a keyframe
    ss = [] if start == 0 else ["-ss", f"{(start - 0.5) / float(rate):.6f}"]
    run(["ffmpeg", "-v", "error", "-nostdin", "-y", "-threads", "1", *ss, "-i", src, "-frames:v", str(count),
         "-map", "0:v:0", "-an", "-sn", "-dn", "-filter_threads", "1", "-c:v", "libsvtav1", "-pix_fmt", "yuv420p10le",
         "-preset", str(preset), "-g", str(gop),
         "-svtav1-params", f"scd=0:irefresh-type=2:lp={LP}:crf={crf:.2f}:{SVT_EXTRA}", *TAGS,
         "-video_track_timescale", str(rate.numerator), *MP4, dst])
    return pathlib.Path(dst).stat().st_size


def secant(points, default_slope):
    """(a, b) of ln(size) = a + b*CRF through the latest point and the latest one at another CRF."""
    c2, s2 = points[-1]
    other = next(((c, s) for c, s in reversed(points[:-1]) if abs(c - c2) > 0.05), None)
    b = (math.log(s2) - math.log(other[1])) / (c2 - other[0]) if other else default_slope
    b = min(-0.01, max(-0.5, b))
    return math.log(s2) - b * c2, b


def solve(models, total_cap, seg_caps):
    """Common CRF with per-segment floors so sum(size) ~= total_cap and size_i <= seg_cap_i."""
    floors = [min(CRF_MAX, max(CRF_MIN, (math.log(cap) - a) / b)) for (a, b), cap in zip(models, seg_caps)]
    size = lambda c: sum(math.exp(a + b * max(c, f)) for (a, b), f in zip(models, floors))
    lo, hi = CRF_MIN, CRF_MAX
    for _ in range(60):
        mid = (lo + hi) / 2
        lo, hi = (lo, mid) if size(mid) <= total_cap else (mid, hi)
    return [max(hi, f) for f in floors]


class Item:
    def __init__(self, src: pathlib.Path, tmp: pathlib.Path):
        self.src, self.name, self.tmp = str(src), src.name, tmp
        self.w, self.h, self.rate, self.frames = probe(self.src)
        self.fps = float(self.rate)
        self.gop = max(1, int(KEYINT_S * self.fps + 1e-9))
        self.budget = BPP * self.w * self.h * self.fps / 1000  # kbps
        seg = self.gop if LP > 1 or CAPPED_SEG_FRAMES <= 0 else max(1, min(self.gop, CAPPED_SEG_FRAMES))
        self.segs = [(a, min(seg, self.frames - a)) for a in range(0, self.frames, seg)]
        window = max(1, round(PEAK_WINDOW_S * self.fps))
        self.total_cap = AIM * self.budget * 1000 / 8 * self.frames / self.fps  # bytes
        # a segment's own share of a window, in proportion to its length; a short tail shares its window with the
        # segment before, so it gets the whole window here and the measured check decides
        last = len(self.segs) - 1
        self.seg_caps = [PEAK_AIM * self.budget * 1000 / 8 * (min(n, window) if i < last or n == seg else window)
                         / self.fps for i, (_, n) in enumerate(self.segs)]
        # the passing file in use: target (not a probe/emergency preset), avg (of budget), path, preset, crfs
        self.kept: dict | None = None
        self.passes = 0
        self.p6_secs = 0.0  # wall time of the latest main-preset pass (the leftover-time estimate)
        self.slopes: list[float] = []  # per segment d ln(size) / d CRF at the main preset

    def encode_pass(self, crfs: list[float], preset: int, polish: bool = False) -> tuple[list[int], dict]:
        self.passes += 1
        t0 = time.monotonic()
        d = self.tmp / f"p{self.passes}"
        d.mkdir()
        parts = [str(d / f"s{i:03d}.mp4") for i in range(len(self.segs))]
        sizes = list(POOL.map(lambda i: encode_segment(self.src, *self.segs[i], self.rate, self.gop, crfs[i], preset,
                                                       parts[i]), range(len(self.segs))))
        if preset == PRESET:
            self.p6_secs = time.monotonic() - t0
        listing = d / "concat.txt"
        listing.write_text("".join(f"file '{p}'\n" for p in parts))
        joined = str(d / "joined.mp4")
        # the concat demuxer rounds each segment's duration, which at 30000/1001 fps leaves a one-frame gap in
        # the timestamps; the validator's CFR canonicalization then adds a frame and refuses the item. Segments
        # and the joined file use a track timescale of the frame-rate numerator, so packet N goes back on the
        # exact grid at N*den (setts' own time_base option is ignored under stream copy). AV1 in MP4: one
        # packet per frame, decode order = display order.
        num, den = self.rate.numerator, self.rate.denominator
        run(["ffmpeg", "-v", "error", "-nostdin", "-y", "-f", "concat", "-safe", "0", "-i", str(listing),
             "-map", "0:v:0", "-c", "copy", "-bsf:v", f"setts=ts=N*{den}:duration={den}",
             "-video_track_timescale", str(num), *TAGS, *MP4, joined])
        avg, peak, gap, first, packets, grid = measure(joined, self.fps, self.frames)
        ok = (avg <= self.budget * AVG_OK and peak <= self.budget * PEAK_OK and first
              and gap <= KEYINT_S + 0.5 / self.fps and packets == self.frames and grid)
        rec = dict(avg=avg / self.budget, peak=peak / self.budget, ok=ok)
        if polish:  # a slower preset wins at about the same budget use
            better = ok and self.kept is not None and rec["avg"] >= self.kept["avg"] - FILL_TOL
        else:  # main search: a main-preset file beats a probe, then more budget used wins
            better = ok and (self.kept is None or (preset <= PRESET, rec["avg"]) > (self.kept["target"], self.kept["avg"]))
        if better:
            if self.kept:
                shutil.rmtree(pathlib.Path(self.kept["path"]).parent, ignore_errors=True)
            self.kept = dict(target=preset <= PRESET, avg=rec["avg"], path=joined, preset=preset, crfs=list(crfs))
        else:
            shutil.rmtree(d, ignore_errors=True)  # /tmp is small: drop every pass not kept
        return sizes, rec

    def encode(self) -> None:
        n = len(self.segs)
        probe_pts = [[] for _ in range(n)]
        sizes, _ = self.encode_pass([35.0] * n, PROBE_PRESET)
        for pts, sz in zip(probe_pts, sizes):
            pts.append((35.0, sz))
        c_probe = min(CRF_MAX - 4, max(CRF_MIN + 4, 35.0 + math.log(sum(sizes) / self.total_cap) / 0.08))
        sizes, _ = self.encode_pass([c_probe] * n, PROBE_PRESET)
        for pts, sz in zip(probe_pts, sizes):
            pts.append((c_probe, sz))
        passes, bump, prev = [], [0.0] * n, None
        max_passes, done = (MAX_PASSES, DONE) if LP > 1 else (CAPPED_MAX_PASSES, CAPPED_DONE)
        for it in range(max_passes + 1):
            if time.monotonic() - START > SOFT_SECONDS:
                log(f"{self.name[:12]}: time is up, keeping what passed")
                break
            if it == max_passes and (self.kept is None or not self.kept["target"] or self.kept["avg"] >= 0.85):
                break  # one extra pass only when the kept file still wastes over 15 % of the budget
            if not passes:
                # first target pass: probe fit, never far from the probes (size(CRF) bends at low CRF)
                crfs = [min(max(c, c_probe - 4), c_probe + 4)
                        for c in solve([secant(p, -0.08) for p in probe_pts], self.total_cap, self.seg_caps)]
            else:
                # interpolate between the latest pass and the closest one on the other side of the target
                last_crfs, last_sizes = passes[-1]
                over = sum(last_sizes) > self.total_cap
                others = [q for q in passes[:-1] if max(abs(x - y) for x, y in zip(q[0], last_crfs)) > 0.05]
                opposite = [q for q in others if (sum(q[1]) > self.total_cap) != over]
                pick = min(opposite or others, key=lambda q: abs(math.log(sum(q[1]) / self.total_cap)), default=None)
                models = []
                for i, pp in enumerate(probe_pts):
                    pts = [(last_crfs[i], last_sizes[i])] if pick is None else [(pick[0][i], pick[1][i]),
                                                                               (last_crfs[i], last_sizes[i])]
                    models.append(secant(pts, secant(pp, -0.08)[1]))
                crfs = [min(max(c, lc - 6), lc + 6)
                        for c, lc in zip(solve(models, self.total_cap, self.seg_caps), last_crfs)]
            crfs = [min(CRF_MAX, c + b) for c, b in zip(crfs, bump)]
            if prev and max(abs(x - y) for x, y in zip(crfs, prev)) < 0.05:
                break
            sizes, rec = self.encode_pass(crfs, PRESET)
            passes.append((crfs, sizes))
            prev = crfs
            if self.kept and self.kept["target"] and self.kept["avg"] >= done:
                break
            if not rec["ok"] and rec["avg"] <= AVG_OK and rec["peak"] > PEAK_OK:
                pair = max(range(n), key=lambda i: sizes[i] + (sizes[i + 1] if i + 1 < n else 0))
                for i in (pair, pair + 1):
                    if i < n:
                        bump[i] += 1.0
        self.slopes = [secant([(q[0][i], q[1][i]) for q in passes], secant(probe_pts[i], -0.08)[1])[1]
                       for i in range(n)] if passes else []

    def polish(self, preset: int, deadline: float) -> bool:
        """Re-encode the kept file at a slower preset, aiming at the same budget use.

        The first pass keeps the CRFs: how much smaller a slower preset is at equal CRF depends on the content
        (q07 at CRF 56 came out larger), so a predicted shift can overshoot. If that pass misses (fails the rules or
        uses less than the kept share minus FILL_TOL) and time allows, a second pass shifts every segment's CRF by one
        common amount, solved from the first pass's own sizes along each segment's measured slope."""
        if not self.kept or not self.kept["target"] or not self.slopes or self.kept["preset"] <= preset:
            return False
        want = min(AIM, self.kept["avg"]) * self.total_cap / AIM  # bytes at the kept share of the budget
        crfs = list(self.kept["crfs"])
        for attempt in (1, 2):
            if attempt == 2 and time.monotonic() + self.p6_secs * TIME_VS_P6[preset] > deadline:
                break
            try:
                sizes, rec = self.encode_pass(crfs, preset, polish=True)
            except Exception as exc:  # noqa: BLE001 - the kept file stays
                log(f"{self.name[:12]}: polish preset {preset} failed: {exc}")
                break
            won = self.kept["preset"] == preset
            log(f"{self.name[:12]}: polish preset {preset} try {attempt} at crf {sum(crfs) / len(crfs):.2f}: "
                f"avg {rec['avg']:.3f} peak {rec['peak']:.3f} {'ok' if rec['ok'] else 'FAILS RULES'}, "
                f"{'kept' if won else 'not kept'}")
            if won:
                return True
            lo, hi = -6.0, 6.0  # common CRF shift d with sum(size_i * exp(b_i * d)) = want
            for _ in range(50):
                mid = (lo + hi) / 2
                lo, hi = (lo, mid) if sum(sz * math.exp(b * mid) for sz, b in zip(sizes, self.slopes)) <= want else (mid, hi)
            crfs = [min(CRF_MAX, max(CRF_MIN, c + hi)) for c in crfs]
        return False

    def emergency(self) -> None:
        """Whole-clip preset-12 encodes at rising CRF until one passes the rules."""
        for crf in (50.0, 58.0, 63.0):
            try:
                self.encode_pass([crf] * len(self.segs), PROBE_PRESET)
            except Exception as exc:  # noqa: BLE001 - keep trying the next CRF
                log(f"{self.name[:12]}: emergency crf {crf} failed: {exc}")
            if self.kept:
                return


def main() -> int:
    in_dir, out_dir = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    srcs = sorted(p for p in in_dir.iterdir() if p.is_file() and not p.name.startswith("."))
    log(f"{len(srcs)} items, {WORKERS} encoders x lp={LP}")
    tmp_root = pathlib.Path(tempfile.mkdtemp(prefix="entry-"))
    items: list[Item] = []
    for src in srcs:
        try:
            items.append(Item(src, pathlib.Path(tempfile.mkdtemp(dir=tmp_root))))
        except Exception as exc:  # noqa: BLE001 - one unreadable item must not stop the batch
            log(f"{src.name[:12]}: probe failed: {exc}")

    def finish(item: Item) -> None:
        part = out_dir / f".partial-{item.name}"
        shutil.copyfile(item.kept["path"], part)
        os.chmod(part, 0o644)
        part.replace(out_dir / item.name)
        log(f"{item.name[:12]}: written, preset {item.kept['preset']}, {item.kept['avg']:.3f} of budget, "
            f"{item.passes} passes")

    def work(item: Item) -> None:
        try:
            item.encode()
        except Exception as exc:  # noqa: BLE001
            log(f"{item.name[:12]}: encode failed: {exc}")
        if item.kept is None:
            item.emergency()
        if item.kept:
            finish(item)
        else:
            log(f"{item.name[:12]}: NO OUTPUT")

    with ThreadPoolExecutor(max(1, len(items))) as clips:
        list(clips.map(work, items))
    try:
        polish_phase(items, finish)
    except Exception as exc:  # noqa: BLE001 - every item already has its output
        log(f"polish phase failed: {exc}")
    shutil.rmtree(tmp_root, ignore_errors=True)
    return 0


def polish_phase(items: list[Item], finish) -> None:
    """Spend the time left before the speed knee on slower presets: hardest items (highest CRF) first, in waves
    that fit the encoder pool, each only if its estimated time fits before the knee."""
    deadline = START + sum(it.frames for it in items) * KNEE_SPF - POLISH_MARGIN_S
    log(f"polish: {deadline - time.monotonic():.1f}s left before the speed knee")
    for preset in POLISH_PRESETS:
        tried: set[str] = set()
        while True:
            now = time.monotonic()
            cands = sorted((it for it in items if it.kept and it.kept["target"] and it.kept["preset"] > preset
                            and it.slopes and it.name not in tried),
                           key=lambda it: -sum(it.kept["crfs"]) / len(it.kept["crfs"]))
            wave, used = [], 0
            for it in cands:
                if now + it.p6_secs * TIME_VS_P6[preset] > deadline or (wave and used + len(it.segs) > WORKERS):
                    continue
                wave.append(it)
                used += len(it.segs)
            if not wave:
                break
            log(f"polish wave, preset {preset}: " + ", ".join(
                f"{it.name[:12]} (crf {sum(it.kept['crfs']) / len(it.kept['crfs']):.1f}, est {it.p6_secs * TIME_VS_P6[preset]:.0f}s)"
                for it in wave))
            tried.update(it.name for it in wave)
            with ThreadPoolExecutor(len(wave)) as ex:
                for it, won in zip(wave, ex.map(lambda it: it.polish(preset, deadline), wave)):
                    if won:
                        finish(it)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 - run.sh's safety net covers any item left without output
        log(f"fatal: {exc}")
        sys.exit(0)
