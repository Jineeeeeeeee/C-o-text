"""
pipeline/base.py — Core types và abstract interfaces cho Lego Blocks Pipeline.

Batch B: Xóa StepConfig/ChainConfig/PipelineConfig.
  Trước: profile["pipeline"] = PipelineConfig.to_dict() → roundtrip qua JSON
         mỗi chapter scrape — root cause bug M4 (nested params silently ignored).
  Sau:   PipelineRunner đọc trực tiếp từ SiteProfile flat fields.
         Zero serialization overhead, zero roundtrip bugs.

Batch B: Xóa abstract to_config() và from_config() khỏi ScraperBlock.
  Không còn cần serialization per-block. execute() vẫn là abstract duy nhất.
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ── Enums ─────────────────────────────────────────────────────────────────────

class BlockType(str, Enum):
    FETCH    = "fetch"
    EXTRACT  = "extract"
    NAVIGATE = "navigate"
    VALIDATE = "validate"
    TITLE    = "title"


class BlockStatus(str, Enum):
    SUCCESS  = "success"
    FALLBACK = "fallback"
    SKIPPED  = "skipped"
    FAILED   = "failed"


# ── RuntimeContext ─────────────────────────────────────────────────────────────

@dataclass
class RuntimeContext:
    """
    Live runtime objects — inject một lần mỗi pipeline execution.
    KHÔNG serialize, KHÔNG lưu disk, KHÔNG put vào SiteProfile.
    """
    pool:       Any = None
    pw_pool:    Any = None
    ai_limiter: Any = None

    @classmethod
    def create(cls, pool: Any, pw_pool: Any, ai_limiter: Any) -> "RuntimeContext":
        return cls(pool=pool, pw_pool=pw_pool, ai_limiter=ai_limiter)

    @classmethod
    def empty(cls) -> "RuntimeContext":
        return cls()

    @property
    def has_pool(self) -> bool:
        return self.pool is not None

    @property
    def has_pw_pool(self) -> bool:
        return self.pw_pool is not None

    @property
    def has_ai(self) -> bool:
        return self.ai_limiter is not None


# ── BlockResult ────────────────────────────────────────────────────────────────

@dataclass
class BlockResult:
    status:      BlockStatus
    data:        Any        = None
    method_used: str        = ""
    confidence:  float      = 1.0
    duration_ms: float      = 0.0
    char_count:  int        = 0
    error:       str | None = None
    metadata:    dict       = field(default_factory=dict)

    @classmethod
    def success(cls, data, method_used="", confidence=1.0, char_count=0, **metadata):
        return cls(
            status      = BlockStatus.SUCCESS,
            data        = data,
            method_used = method_used,
            confidence  = confidence,
            char_count  = char_count or (len(data) if isinstance(data, str) else 0),
            metadata    = metadata,
        )

    @classmethod
    def fallback(cls, data, method_used="fallback", confidence=0.6, **metadata):
        return cls(
            status      = BlockStatus.FALLBACK,
            data        = data,
            method_used = method_used,
            confidence  = confidence,
            char_count  = len(data) if isinstance(data, str) else 0,
            metadata    = metadata,
        )

    @classmethod
    def failed(cls, error: str, method_used: str = "") -> "BlockResult":
        return cls(status=BlockStatus.FAILED, error=error, method_used=method_used, confidence=0.0)

    @classmethod
    def skipped(cls, reason: str = "") -> "BlockResult":
        return cls(status=BlockStatus.SKIPPED, metadata={"reason": reason})

    @property
    def ok(self) -> bool:
        return self.status in (BlockStatus.SUCCESS, BlockStatus.FALLBACK)

    @property
    def is_primary(self) -> bool:
        return self.status == BlockStatus.SUCCESS


# ── PipelineContext ────────────────────────────────────────────────────────────

@dataclass
class PipelineContext:
    url:      str
    profile:  dict = field(default_factory=dict)
    progress: dict = field(default_factory=dict)
    runtime:  RuntimeContext = field(default_factory=RuntimeContext.empty)

    html:         str | None = None
    status_code:  int        = 0
    fetch_method: str        = ""
    soup:         Any        = None

    content:       str | None = None
    title_raw:     str | None = None
    title_clean:   str | None = None
    selector_used: str | None = None

    next_url:   str | None = None
    nav_method: str        = ""

    is_valid:         bool  = False
    validation_score: float = 0.0
    validation_notes: list  = field(default_factory=list)

    detected_js_heavy: bool = False

    block_results:     dict  = field(default_factory=dict)
    total_duration_ms: float = 0.0
    errors:            list  = field(default_factory=list)

    def record(self, block_name: str, result: BlockResult) -> None:
        self.block_results[block_name] = result
        self.total_duration_ms += result.duration_ms
        if result.status == BlockStatus.FAILED and result.error:
            self.errors.append(f"{block_name}: {result.error}")

    def get_pipeline_score(self) -> dict[str, float]:
        quality  = self.validation_score
        speed_ms = max(self.total_duration_ms, 1)
        speed    = min(1.0, max(0.0, 1.0 - (speed_ms - 500) / 4500))
        used_pw  = "playwright" in self.fetch_method.lower()
        resource = 0.5 if used_pw else 1.0
        confs    = [r.confidence for r in self.block_results.values() if r.ok and r.confidence > 0]
        confidence = sum(confs) / len(confs) if confs else 0.0
        total = 0.4 * quality + 0.3 * speed + 0.2 * resource + 0.1 * confidence
        return {
            "quality":    round(quality,    3),
            "speed":      round(speed,      3),
            "resource":   round(resource,   3),
            "confidence": round(confidence, 3),
            "total":      round(total,      3),
        }


# ── Abstract base class ───────────────────────────────────────────────────────

class ScraperBlock(ABC):
    """
    Batch B: Không còn abstract to_config() và from_config().
    execute() là abstract method duy nhất.
    """
    block_type: BlockType = BlockType.FETCH
    name:       str       = "base_block"

    @abstractmethod
    async def execute(self, ctx: PipelineContext) -> BlockResult: ...

    def _timed(self, result: BlockResult, start: float) -> BlockResult:
        result.duration_ms = (time.monotonic() - start) * 1000
        return result