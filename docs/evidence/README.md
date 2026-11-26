# 证据

| 文件 | 内容 |
| --- | --- |
| `e2e-capture-20261126.jsonl` | 真 Gateway + 桩上游的一次完整 e2e 抓包（脱敏） |

抓包由 `e2e/run_e2e.py` 产生、`e2e/make_evidence.py` 脱敏：只保留路径、解码后的
`X-Agent-Hint`、消息条数/角色序列、工具名与 `stream` 标志；**不含**凭据、端点、
消息内容与请求体原文。

复现：

```bash
bash e2e/run.sh
LATEST=$(ls -dt e2e/.tmp/home-*/captures.jsonl | head -1)
PYTHONPATH=src <deer-flow venv>/python e2e/make_evidence.py "$LATEST" docs/evidence/e2e-capture-<date>.jsonl
```
