# Base Python
FROM python:3.11-slim

# Cartella di lavoro
WORKDIR /app

# Copia requirements e installa dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice
COPY src/ ./src/


# Comando di default
CMD ["bash"]
#CMD ["python", "src/edinet_scraper.py"]