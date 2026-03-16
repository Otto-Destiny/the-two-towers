FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /srv/app

COPY pyproject.toml ./pyproject.toml
COPY guides ./guides
COPY watchtower ./watchtower

RUN pip install --no-cache-dir .

EXPOSE 8080

CMD ["python", "-m", "watchtower.app"]
