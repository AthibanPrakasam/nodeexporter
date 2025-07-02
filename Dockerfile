# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first (for caching layer)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY nodemetrics.py .

# Expose the port your FastAPI app runs on
EXPOSE 8000

# Run FastAPI with uvicorn
CMD ["uvicorn", "nodemetrics:app", "--host", "0.0.0.0", "--port", "8000"]
