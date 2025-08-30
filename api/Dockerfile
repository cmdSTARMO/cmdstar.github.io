# 基于官方 FastAPI 镜像
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10

# （可选）把工作目录切到 /app/api，这样后面启动命令导入模块更直观
WORKDIR /app/api

# 把 api/ 下的所有文件（包括 data/）都复制进来
COPY . /app/api

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# ──
# 下面这行就没必要了，因为你并不再往 /app/data 放东西
# RUN mkdir -p /app/data
# ──

# 镜像自带的默认启动命令会自动以 uvicorn main:app 启动，
# 如果你改了 WORKDIR，确保它跑的是 `main:app` 而不是 `api.main:app`
