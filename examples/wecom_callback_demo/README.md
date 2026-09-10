# WeCom（企业微信）真实回调协议 Demo

展示如何用 `trpc_agent_sdk.tenants` 接入**真实的企业微信自建应用回调协议**（区别于测试用的模拟协议）：

- `GET /wecom/callback` — URL 验证：解密 `echostr` 并原文返回明文
- `POST /wecom/callback` — 消息回调：`msg_signature` 验签 + AES-256-CBC 解密 + XML 解析 → 多租户路由（corp_id）→ 白名单/去重/审计 → 5 秒内应答 `"success"`，回复在后台异步发送

## 1. 企业微信后台配置

在企业微信管理后台（[work.weixin.qq.com](https://work.weixin.qq.com)）：

1. 进入 **应用管理 → 创建应用**（自建应用），记下：
   - **CorpID**：我的企业 → 企业信息 → 企业ID → 对应 `WECOM_CORP_ID`
   - **Secret**：应用详情页 → 对应 `WECOM_CORP_SECRET`
   - **AgentId**：应用详情页 → 对应 `WECOM_AGENT_ID`
2. 在应用详情页进入 **接收消息 → 设置API接收**：
   - **URL**：`https://你的域名/wecom/callback`（必须可公网访问；本地调试用内网穿透，如 frp/ngrok）
   - **Token**：自定义或随机生成 → 对应 `WECOM_CALLBACK_TOKEN`
   - **EncodingAESKey**：随机生成（43 字符）→ 对应 `WECOM_AES_KEY`
   - 保存时企业微信会立即发起 **GET URL 验证**，服务必须在 5 秒内正确响应

## 2. 安装与运行

```bash
pip install "trpc-agent-py[wecom]"   # 或 pip install cryptography fastapi uvicorn
# demo 依赖
pip install fastapi uvicorn

# 设置环境变量（或直接使用 demo 默认值）
export WECOM_CORP_ID=ww_demo_corp_id
export WECOM_CORP_SECRET=your_secret
export WECOM_AGENT_ID=1000002
export WECOM_CALLBACK_TOKEN=your_token
export WECOM_AES_KEY=your_43_char_encoding_aes_key

uvicorn main:app --host 0.0.0.0 --port 8000
```

ChannelConfig 字段映射：

| 企业微信后台参数 | `ChannelConfig` 字段 | 用途 |
|---|---|---|
| CorpID（企业ID） | `bot_id` | 明文 `<ToUserName>` → 多租户路由 |
| 应用 Secret | `api_key` | 主动发消息（message/send）取 access_token |
| AgentId | `agent_id` | 主动发消息的目标应用 |
| 接收消息 Token | `webhook_token` | `msg_signature` 验签 |
| EncodingAESKey | `webhook_secret` | AES-256-CBC 解密 |

> 注意：真实协议下 `webhook_token`/`webhook_secret` 的含义与旧的模拟协议不同（模拟协议里 `webhook_secret` 只是普通签名密钥）。同一个租户不要混用两种协议。

## 3. 本地自测（无需企业微信后台）

用 SDK 自带的 `WeComCrypto` 构造合法回调报文打本地服务：

```python
import asyncio, httpx
from trpc_agent_sdk.tenants import WeComCrypto

crypto = WeComCrypto("demo_callback_token",
                     "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ",
                     "ww_demo_corp_id")

async def main():
    # GET URL 验证
    echostr, sig = crypto.encrypt("echo_plain_text", "1700000000", "n1")
    r = await httpx.AsyncClient().get(
        "http://127.0.0.1:8000/wecom/callback",
        params={"msg_signature": sig, "timestamp": "1700000000", "nonce": "n1", "echostr": echostr})
    print("GET:", r.status_code, r.text)  # 200 echo_plain_text

    # POST 消息回调
    inner = ("<xml><ToUserName><![CDATA[ww_demo_corp_id]]></ToUserName>"
             "<FromUserName><![CDATA[u1]]></FromUserName>"
             "<CreateTime>1700000000</CreateTime>"
             "<MsgType><![CDATA[text]]></MsgType>"
             "<Content><![CDATA[hello]]></Content>"
             "<MsgId>10001</MsgId>"
             "<AgentID>1000002</AgentID></xml>")
    encrypt, sig = crypto.encrypt(inner, "1700000000", "n1")
    body = (f"<xml><ToUserName><![CDATA[ww_demo_corp_id]]></ToUserName>"
            f"<Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>")
    r = await httpx.AsyncClient().post(
        "http://127.0.0.1:8000/wecom/callback",
        params={"msg_signature": sig, "timestamp": "1700000000", "nonce": "n1"},
        content=body.encode())
    print("POST:", r.status_code, r.text)  # 200 success

asyncio.run(main())
```

## 4. 行为说明与生产建议

- **5 秒应答**：POST 在解密/治理完成后立即返回 `"success"`；agent 回复在后台任务中通过 `message/send` 主动发送（`TenantChannelManager.send_response`）。
- **重试与去重**：企业微信对超时/非 `success` 响应重试 3 次；`MessageDeduplicator` 按 `MsgId` 去重兜底。
- **后台任务不重试**：进程崩溃时后台回复丢失；生产建议把消息投递到外部队列（如 Kafka/MQ）由独立 worker 消费。
- **回复入口**：demo 用 `TenantRunner` 替换 `main.py` 中 `handle_message` 的 echo 逻辑即可接入真实 agent。
- **群聊**：应用号消息带 `ChatId` 字段时标记为群聊（`TenantMessage.is_group_chat`），可用 `ChannelConfig.session_strategy="group"` 让群共享会话。
- **多租户**：每个租户配置自己的 `bot_id`（corp_id）+ Token + EncodingAESKey，按明文 `<ToUserName>` 路由；同一 corp 的多个自建应用可用 `agent_id` 字段区分。
