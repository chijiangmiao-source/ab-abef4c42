FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/data/calibration.db

WORKDIR /app

# 零第三方依赖：仅复制应用代码与页面，无需 pip install。
COPY app ./app
COPY web ./web
COPY tests ./tests

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --retries=10 --start-period=3s \
  CMD python -c "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=3); sys.exit(0 if r.status==200 else 1)"

CMD ["python", "-m", "app.server"]
