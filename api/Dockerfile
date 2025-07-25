# 基于官方 FastAPI 镜像
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10

# 把代码复制进 /app（镜像默认工作目录）
COPY . /app

# 安装依赖
RUN pip install --no-cache-dir -r /app/requirements.txt

# 确保 data 目录可访问（如果你的 DB 在 ../data）
RUN mkdir -p /app/data
# （如果要把宿主机 data 挂载进来可在运行时指定 -v）

# 默认启动命令无需改，镜像会自动跑:
# uvicorn main:app --host 0.0.0.0 --port 80
