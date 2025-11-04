FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    libpoppler-cpp-dev \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-chi-tra \
    ghostscript \
    imagemagick \
    gcc \
    g++ \
    curl \
 && rm -rf /var/lib/apt/lists/*


# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip uninstall -y urllib || true
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

# Copy project code
COPY . .

# Create necessary folders
RUN mkdir -p logs uploads static/user_logos

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=production
ENV PYTHONPATH=/app
ENV PORT=8080
EXPOSE 8080

# Optional: Create a non-root user for security
ARG USER_ID=1001
ARG GROUP_ID=1001
RUN groupadd -g $GROUP_ID appuser && \
    useradd -u $USER_ID -g $GROUP_ID -m -s /bin/bash appuser && \
    chown -R appuser:appuser /app && \
    chmod -R 755 /app
USER appuser

# Optional health check (Cloud Run/Render handles this automatically)
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/login || exit 1

# Run Flask app with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--threads", "2", "--timeout", "300", "--access-logfile", "-", "--error-logfile", "-", "app:app"]
