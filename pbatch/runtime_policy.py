"""AADM-R §21 运行时策略：声明式渐进执行内核配置。

默认从最低充分模式开始（start_mode 是下界）；升级只由证据驱动（profile
L0-L3 路由 + 硬触发器）。解析放在独立模块以控制 config.py 行数；
`from .runtime_policy import ...` 或经 config re-export 均可。
"""

from __future__ import annotations

from .config import _bool_setting, _int_setting, _section

_RUNTIME_CFG = _section("runtime_policy")
RUNTIME_START_MODE = _int_setting(_RUNTIME_CFG, "start_mode", 0, 0)
RUNTIME_AUTO_ESCALATE = _bool_setting(_RUNTIME_CFG, "auto_escalate", True)
RUNTIME_AUTO_DEESCALATE = _bool_setting(_RUNTIME_CFG, "auto_deescalate", True)
_RUNTIME_RESOURCES = _RUNTIME_CFG.get("resources")
if not isinstance(_RUNTIME_RESOURCES, dict):
    _RUNTIME_RESOURCES = {}
RUNTIME_RESOURCE_OBSERVE = _bool_setting(
    _RUNTIME_RESOURCES, "observe", True)
RUNTIME_RESOURCE_AFFECT_SOLUTION = _bool_setting(
    _RUNTIME_RESOURCES, "affect_solution", False)
_RUNTIME_AGENTS = _RUNTIME_CFG.get("agents")
if not isinstance(_RUNTIME_AGENTS, dict):
    _RUNTIME_AGENTS = {}
RUNTIME_AGENT_DEFAULT_COUNT = _int_setting(
    _RUNTIME_AGENTS, "default_count", 1, 1)
RUNTIME_MAX_PARALLELISM = _int_setting(
    _RUNTIME_AGENTS, "max_parallelism", 4, 1)
_RUNTIME_STRATEGIES = _RUNTIME_CFG.get("strategies")
if not isinstance(_RUNTIME_STRATEGIES, dict):
    _RUNTIME_STRATEGIES = {}
RUNTIME_MAX_PATHS = _int_setting(_RUNTIME_STRATEGIES, "max_paths", 3, 1)
RUNTIME_DEFAULT_PATHS = _int_setting(
    _RUNTIME_STRATEGIES, "default_paths", 1, 1)
_RUNTIME_QUALITY = _RUNTIME_CFG.get("quality")
if not isinstance(_RUNTIME_QUALITY, dict):
    _RUNTIME_QUALITY = {}
RUNTIME_NEVER_SKIP_PROOFS = _bool_setting(
    _RUNTIME_QUALITY, "never_skip_required_proofs", True)
RUNTIME_NEVER_CLAIM_UNEXECUTED = _bool_setting(
    _RUNTIME_QUALITY, "never_claim_unexecuted_tests_passed", True)


def runtime_policy_view() -> dict:
    """AADM-R §21 运行时策略的结构化视图（供 profile 注入）。"""
    return {
        "start_mode": RUNTIME_START_MODE,
        "auto_escalate": RUNTIME_AUTO_ESCALATE,
        "auto_deescalate": RUNTIME_AUTO_DEESCALATE,
        "agents": {"default_count": RUNTIME_AGENT_DEFAULT_COUNT,
                   "max_parallelism": RUNTIME_MAX_PARALLELISM},
        "strategies": {"default_paths": RUNTIME_DEFAULT_PATHS,
                       "max_paths": RUNTIME_MAX_PATHS},
        "resources": {"observe": RUNTIME_RESOURCE_OBSERVE,
                      "affect_solution": RUNTIME_RESOURCE_AFFECT_SOLUTION},
        "quality": {"never_skip_required_proofs": RUNTIME_NEVER_SKIP_PROOFS,
                    "never_claim_unexecuted_tests_passed":
                        RUNTIME_NEVER_CLAIM_UNEXECUTED},
    }
