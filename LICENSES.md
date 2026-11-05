# Third-party components

This entry's own code (`run.sh`, `streaming_entry.py`, `Dockerfile`) is released under the Apache License 2.0
(`LICENSE`). Everything else it ships is listed below with its source and licence. The entry downloads nothing at
run time and ships no model weights.

## Container base and binaries

| Component | Source | Licence | Location |
|---|---|---|---|
| Python 3.13 official slim image (Debian 12 bookworm), pinned by digest | https://hub.docker.com/_/python | PSF-2.0 (Python); Debian packages under their own free licences | image base (`FROM`) |
| FFmpeg 9.0 / FFprobe static build (`--enable-gpl --enable-version3`, no `--enable-nonfree`), pinned by digest | https://github.com/wader/static-ffmpeg | GPL-3.0-or-later | `/usr/local/bin/ffmpeg`, `/usr/local/bin/ffprobe` |
| SVT-AV1 4.2.0, statically linked into the FFmpeg build above (the only encoder this entry uses) | https://gitlab.com/AOMediaCodec/SVT-AV1 | BSD-3-Clause-Clear, plus the Alliance for Open Media Patent License 1.0 | inside `/usr/local/bin/ffmpeg` |

## Data used for tuning

The encoder settings (preset, quantization matrices, rate-control constants) were chosen on the VidAIO
`practice-pack-005` clips, which VidAIO publishes under CC0 (the screen clips show CPython source under the
PSF License 2.0). No other data was used to train or tune this entry.
