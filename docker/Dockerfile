FROM python:3.13-slim
RUN pip install --no-cache-dir PyYAML==6.0.3 aiohttp==3.13.3
WORKDIR /app
COPY docker/cli-proxy-api /usr/local/bin/cli-proxy-api
RUN chmod 755 /usr/local/bin/cli-proxy-api
COPY scripts/controller.py /app/scripts/controller.py
COPY scripts/router.py /app/scripts/router.py
COPY scripts/bootstrap_open_source.py /app/scripts/bootstrap_open_source.py
COPY web /app/web
ENV PYTHONUNBUFFERED=1 TZ=Asia/Shanghai
CMD ["python", "/app/scripts/controller.py"]
