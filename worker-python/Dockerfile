FROM python:3.12-slim
WORKDIR /app
COPY worker-python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || true
COPY worker-python/ .
# Uygulamanın giriş noktasını senin dosyana göre ayarla:
CMD ["python", "-m", "app"]
