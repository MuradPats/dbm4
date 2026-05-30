FROM apache/airflow:2.9.3

USER airflow

# Install torch + torchvision as a matched pair from the CPU-only wheel index.
# Mismatched versions cause "operator torchvision::nms does not exist" at import.
RUN pip install --no-cache-dir \
        "torch==2.2.2" \
        "torchvision==0.17.2" \
        --index-url https://download.pytorch.org/whl/cpu

# Install remaining pipeline runtime dependencies.
COPY requirements-pipeline.txt /requirements-pipeline.txt
RUN pip install --no-cache-dir -r /requirements-pipeline.txt
