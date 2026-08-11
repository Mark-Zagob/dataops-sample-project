# =================================================================
# PRODUCTION-GRADE DOCKERFILE
# =================================================================
# Sử dụng slim image để giảm attack surface và dung lượng.
# Không dùng "latest" - luôn pin version cụ thể.
FROM python:3.10-slim

# Đặt working directory bên trong container
WORKDIR /opt/dagster

# Cài đặt system dependencies cần thiết cho psycopg2
# libpq-dev: thư viện C để compile psycopg2
# gcc: compiler
# Sau khi cài xong, xóa cache apt để giảm dung lượng image
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt TRƯỚC khi copy code.
# Đây là kỹ thuật Docker Layer Caching:
# Nếu requirements.txt không đổi, Docker sẽ dùng lại cache pip install
# từ lần build trước, tiết kiệm rất nhiều thời gian.
COPY platform/dagster/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy Dagster configuration files
COPY platform/dagster/dagster.yaml ./dagster_home/dagster.yaml
COPY platform/dagster/workspace.yaml ./dagster_home/workspace.yaml

# Copy User Code (Production Mode: COPY, không mount volume)
# Mọi thay đổi code đều yêu cầu build lại image để đảm bảo Immutability.
COPY user_code/ ./user_code/

# Tạo thư mục DAGSTER_HOME nếu chưa tồn tại
# Đây là nơi Dagster lưu trữ run history, logs, schedules
RUN mkdir -p /opt/dagster/dagster_home

# Set biến môi trường cho Dagster
ENV DAGSTER_HOME=/opt/dagster/dagster_home
# Đảm bảo Python không buffer output, giúp log hiển thị realtime trong Docker
ENV PYTHONUNBUFFERED=1

# Expose port cho Dagster Webserver (UI)
EXPOSE 3000

# Healthcheck cho container Dagster
# SRE LENS: Docker sẽ tự động kiểm tra UI có đang sống không
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:3000/health')" || exit 1

# Lệnh khởi chạy khi container start
# -w: trỏ đến file workspace.yaml để Dagster biết nơi load code
# -h 0.0.0.0: listen trên tất cả interfaces (cần thiết để Docker expose port ra ngoài)
# -p 3000: port của Webserver
CMD ["dagster-webserver", "run", \
     "-h", "0.0.0.0", \
     "-p", "3000", \
     "-w", "/opt/dagster/dagster_home/workspace.yaml"]