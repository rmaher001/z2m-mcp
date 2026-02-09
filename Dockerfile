FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY app/ app/

RUN pip install --no-cache-dir .

VOLUME /data/logs

ENV MCP_TRANSPORT=sse
EXPOSE 8000

CMD ["python", "-m", "app"]
