"""问题系统分类学 → 方法论路由表（哥德尔启发）。

classifier 检测 system_type（state-machine/event-driven/...），本表把
系统类型映射到方法论规范文件——先判定问题属于哪类系统，再注入该类
系统的方法论（即使任务文本没有对应关键词）。
"""

SYSTEM_TYPE_METHODOLOGY: dict = {
    "state-machine": [
        "backend-specs/design-intelligence/02-domain-lifecycle.md",
        "backend-specs/domain-modeling.md",
    ],
    "event-driven": [
        "backend-specs/design-intelligence/02-domain-lifecycle.md",
        "backend-specs/system-engineering.md",
    ],
    "realtime": [
        "backend-specs/design-intelligence/04-experience-architecture.md",
        "backend-specs/network-engineering.md",
    ],
    "search": [
        "backend-specs/algorithms-data-structures.md",
    ],
    "optimization": [
        "backend-specs/algorithms-data-structures.md",
        "backend-specs/complexity-and-scale.md",
    ],
    "knowledge": [
        "backend-specs/algorithms-data-structures.md",
        "backend-specs/design-intelligence/03-ui-driven-data-model.md",
    ],
    "batch": [
        "backend-specs/system-engineering.md",
        "backend-specs/design-intelligence/04-experience-architecture.md",
    ],
    "adaptive": [
        "backend-specs/design-intelligence/04-experience-architecture.md",
        "backend-specs/design-intelligence/05-operation-intelligence.md",
    ],
    "collaboration": [
        "backend-specs/design-intelligence/01-experience-driven-api.md",
        "backend-specs/architecture-constitution.md",
    ],
    "deterministic": [],
}
