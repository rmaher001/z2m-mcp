FROM python:3.12-slim

LABEL io.docker.server.metadata='{"name":"z2m-mcp","description":"Zigbee2MQTT diagnostics and control MCP server","longLived":true,"command":["python","-m","app"],"secrets":[{"name":"z2m-mcp.mqtt_host","env":"MQTT_HOST"},{"name":"z2m-mcp.mqtt_port","env":"MQTT_PORT"},{"name":"z2m-mcp.mqtt_username","env":"MQTT_USERNAME"},{"name":"z2m-mcp.mqtt_password","env":"MQTT_PASSWORD"},{"name":"z2m-mcp.tz","env":"TZ"}]}'

WORKDIR /app

COPY pyproject.toml .
COPY app/ app/

RUN pip install --no-cache-dir .

VOLUME /data/logs

ENV MCP_TRANSPORT=sse
EXPOSE 8000

CMD ["python", "-m", "app"]
