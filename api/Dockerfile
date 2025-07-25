# 基于官方 FastAPI 镜像
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10

# 设置工作目录
WORKDIR /app

# 复制 API 代码
COPY ./api /app

# 复制数据库文件
COPY ./data /app/data

# 安装依赖
RUN pip install --no-cache-dir -r /app/requirements.txt

# （如果要把宿主机 data 挂载进来可在运行时指定 -v）

# 默认启动命令无需改，镜像会自动跑:
# uvicorn main:app --host 0.0.0.0 --port 80
