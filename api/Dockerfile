# --- Base image ---
FROM python:3.11-slim

# --- Working directory inside container ---
WORKDIR /app

# --- Copy requirements and install ---
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Copy app source ---
COPY api .

# -- COPY DATA FOLDER ; REPLACE LATER WITH s3 LOGIC --
COPY data ./data

# --- Expose FastAPI port ---
EXPOSE 8080

# --- Start server ---
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
