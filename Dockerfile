# syntax=docker/dockerfile:1
FROM python:3.10-alpine
RUN apk add --no-cache gcc musl-dev linux-headers libffi-dev g++ libpq-dev
RUN pip install --upgrade pip

COPY scraper_root/requirements.txt /scraper/scraper_root/requirements.txt
RUN pip install -r /scraper/scraper_root/requirements.txt

COPY scraper_root /scraper/scraper_root

WORKDIR /scraper
ENV PYTHONPATH "${PYTHONPATH}:/scraper"
CMD ["python3", "-u", "scraper_root/scraper.py"]
