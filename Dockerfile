FROM python@sha256:00faa2debb87529f9f0764e9491d8ba400a3678976616c3bd7cb193745ac20d1
COPY --from=mwader/static-ffmpeg@sha256:b90574a4e2ae62b763c39c384526689e7eb435da6398f4fb3f6c3f1c6a14ce33 /ffmpeg /ffprobe /usr/local/bin/
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 SVT_LOG=1
WORKDIR /app
COPY run.sh streaming_entry.py /app/
RUN chmod 0555 /app/run.sh && chmod 0444 /app/streaming_entry.py && python3 -c "import streaming_entry" \
 && ffmpeg -hide_banner -encoders 2>/dev/null | grep -q libsvtav1 && ffprobe -version >/dev/null \
 && command -v timeout >/dev/null && command -v awk >/dev/null
