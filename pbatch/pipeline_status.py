"""Pipeline outcome statuses (central state set).

散落字符串的拼写错误无编译期保护（检查器 status_literals_without_state_set
抓到的真实技术债）。集中定义后所有判定/比较/记录引用常量。
"""


class PipelineStatus:
    PASSED = "PASSED"
    GATE_REJECTED = "GATE_REJECTED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    PIPELINE_FAILED = "PIPELINE_FAILED"
