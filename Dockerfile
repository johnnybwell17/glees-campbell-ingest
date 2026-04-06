FROM waggle/plugin-base:1.1.1-base

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY config /app/config

ENTRYPOINT ["python3", "/app/app/main.py"]
