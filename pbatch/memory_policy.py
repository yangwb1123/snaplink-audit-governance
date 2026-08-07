"""Lightweight prompt/domain policy for progressive message memory."""

from __future__ import annotations

import re


_DOMAINS = {
    "authentication": ("auth", "login", "logout", "mfa", "identity", "认证", "登录", "身份"),
    "security": ("security", "threat", "attack", "secret", "权限", "安全", "攻击", "漏洞"),
    "protocol": ("protocol", "wire", "packet", "rfc", "协议", "报文", "兼容"),
    "database": ("database", "schema", "sql", "storage", "数据库", "存储", "迁移"),
    "frontend": ("frontend", "ui", "ux", "react", "页面", "界面", "交互"),
    "infrastructure": ("deploy", "docker", "kubernetes", "network", "部署", "运维", "网络"),
    "testing": ("test", "qa", "coverage", "validator", "gate", "测试", "验收", "门禁"),
    "performance": ("performance", "latency", "throughput", "性能", "延迟", "吞吐"),
    "documentation": ("document", "readme", "docs", "文档", "说明"),
    "product": ("requirement", "feature", "product", "需求", "功能", "产品"),
}
_RESUME_WORDS = (
    "continue", "previous", "history", "resume", "继续", "之前", "历史", "上次", "复用")
_STRONG_RESUME_WORDS = ("continue", "resume", "继续", "上次", "复用")
_EXECUTE_WORDS = (
    "implement", "fix", "change", "build", "review", "实现", "修复", "修改", "评审", "重构")


def classify_prompt(prompt: str, mode_hint: str = "") -> dict:
    """Classify a prompt without retrieval or an LLM call."""
    folded = " ".join((prompt or "").casefold().split())
    domains = [name for name, terms in _DOMAINS.items()
               if any(term in folded for term in terms)]
    explicit_execute = any(word in folded for word in _EXECUTE_WORDS)
    short_question = len(folded) < 120 and ("?" in prompt or "？" in prompt)
    resume_hint = any(word in folded for word in _RESUME_WORDS)
    strong_resume = any(word in folded for word in _STRONG_RESUME_WORDS)
    if strong_resume or (resume_hint and not short_question):
        mode = "resume"
    elif explicit_execute:
        mode = "execute"
    elif short_question:
        mode = "direct"
    elif re.search(r"\.[a-z0-9]{1,8}\b", folded):
        mode = "execute"
    else:
        mode = "discover"
    if mode_hint in ("execute", "resume"):
        mode = mode_hint
    return {"mode": mode, "domains": domains}
