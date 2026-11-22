FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN --mount=type=secret,id=pip_ca \
    if [ -f /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi; \
    pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["python", "app.py"]
