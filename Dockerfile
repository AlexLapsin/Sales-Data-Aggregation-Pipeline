FROM python:3.13-slim

# Update Debian packages & install Python deps in one layer
RUN apt-get update \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      # any OS deps you actually need, e.g. gcc if you build wheels
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash"]
