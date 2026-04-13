# 洛克王国远行商人商品自动监控

纯后端项目：抓取网页内容 → 结构化解析 → FastAPI 提供扫描接口 → 命中规则后触发通知。

## 功能概览
- 抓取配置中的目标网页内容
- 对抓取结果做结构化解析
- 通过 `/api/scan` 手动触发一次扫描
- 命中关键词后执行外部通知命令
- 提供 `/health` 健康检查接口

## 技术栈
- Python 3
- FastAPI
- Requests
- LLM 结构化解析
- OpenClaw 消息发送

## 配置
复制 `config.example.json` 为 `config.json` 后按需修改：
- `server.host` / `server.port`：监听地址和端口
- `polling.interval_seconds`：预留的轮询时间间隔配置
- `fetch.sources`：要抓取的网页列表
- `fetch.headers`：抓取时附带的请求头
- `llm.base_url` / `llm.api_key` / `llm.model`：LLM 服务配置
- `listen`：需要监听的关键词列表
- `notify.enabled` / `notify.command`：通知开关和通知命令

`config.json` 已加入 `.gitignore`，避免把本地敏感配置提交到公共仓库。

## 安装与启动
```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
python app.py
```

## 接口
- `GET /health`：健康检查
- `POST /api/scan`：手动触发一次抓取和规则判断

示例：
```bash
curl http://127.0.0.1:8000/health
curl -X POST http://127.0.0.1:8000/api/scan
```

## 项目结构
- `app.py`：本地启动入口
- `app/main.py`：FastAPI 应用装配
- `app/routers/`：路由层
- `app/services/`：抓取、解析、规则、通知等服务
- `config.example.json`：示例配置
- `config.json`：本地运行配置（已忽略）
- `data/`：运行产生的数据目录（已忽略）

## 说明
当前仓库默认暴露的是手动扫描接口，通知会复用已配置好的 OpenClaw channel。
