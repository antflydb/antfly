FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
COPY pyproject.toml /app/
COPY src /app/src
RUN pip install --no-cache-dir --no-deps . && useradd --uid 10001 --create-home gateway
USER gateway
EXPOSE 8089
ENTRYPOINT ["antfly-lake-maintenance", "--host", "0.0.0.0", "--config", "/config/controller.json"]
