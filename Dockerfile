# One image for every service. The command picks which one runs:
#   docker run orderpipeline api | relay | payments | inventory
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 app && mkdir /data && chown app /data
COPY --from=builder --chown=app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    ORDERPIPELINE_DB_PATH=/data/orderpipeline.db \
    ORDERPIPELINE_SERVICE=api
USER app
WORKDIR /home/app
EXPOSE 8000
# The API answers /healthz; workers report a heartbeat file written each loop.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD ["orderpipeline", "healthcheck"]
ENTRYPOINT ["orderpipeline"]
CMD ["api", "--port", "8000"]
