"""
pipeline/base.py — Core types và abstract interfaces cho Lego Blocks Pipeline.

Fix P1-8: làm rõ to_config() là debug/introspection helper, KHÔNG phải
  serialization path. Serialization thực sự đi qua StepConfig.to_dict().

  Trước: abstract method bắt buộc nhưng không có call site nào trong
  serialization path → developer sửa to_config() sẽ không có tác dụng gì,
  gây confusion và wasted effort.

  Sau: to_config() giữ nguyên nhưng docstring + comment rõ ràng:
    - Mục đích: debug / introspection / logging
    - KHÔNG được gọi bởi executor hay profile persistence
    - Serialization pipeline: StepConfig.to_dict() → ChainConfig.to_dict()
      → PipelineConfig.to_dict() → profile JSON

  Không xóa abstract requirement vì to_config() vẫn hữu ích để inspect
  block config lúc runtime (VD: in ra pipeline đang dùng để debug).
  Nhưng developer cần biết rõ context nó được dùng ở đâu.
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
    block_type: BlockType = BlockType.FETCH
    name:       str       = "base_block"

    @abstractmethod
    async def execute(self, ctx: PipelineContext) -> BlockResult: ...

    @abstractmethod
    def to_config(self) -> dict:
        """
        Trả về config dict của block này.

        Fix P1-8: ĐÂY LÀ DEBUG/INTROSPECTION HELPER — không phải serialization path.

        Serialization thực sự:
            StepConfig.to_dict() → ChainConfig.to_dict() → PipelineConfig.to_dict()
            → profile JSON trên disk

        to_config() KHÔNG được gọi bởi executor hay profile persistence.
        Dùng để: logging, debugging, in ra pipeline config đang chạy.

        Nếu bạn sửa to_config() để thay đổi cách block được lưu vào profile,
        sửa đó sẽ KHÔNG có tác dụng. Bạn cần sửa StepConfig.to_dict() và
        tương ứng from_config() thay vào đó.
        """
        ...

    @classmethod
    @abstractmethod
    def from_config(cls, config: dict) -> "ScraperBlock": ...

    def _timed(self, result: BlockResult, start: float) -> BlockResult:
        result.duration_ms = (time.monotonic() - start) * 1000
        return result


# ── StepConfig ────────────────────────────────────────────────────────────────

@dataclass
class StepConfig:
    """
    Config cho một step trong chain.

    Fix M4: to_dict() dùng nested "params" key thay vì flat merge.
    Format mới (v2): {"type": "selector", "params": {"selector": "div.content"}}
    Format cũ (v1):  {"type": "selector", "selector": "div.content"}
    Backward compat qua from_legacy_dict().
    """
    type:   str
    params: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if "type" in self.params:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "[StepConfig] params có key 'type' — có thể gây nhầm lẫn. "
                "step.type=%r, params['type']=%r",
                self.type, self.params["type"],
            )

    def to_dict(self) -> dict:
        return {"type": self.type, "params": dict(self.params)}

    @classmethod
    def from_dict(cls, d: dict) -> "StepConfig":
        if "params" in d:
            return cls(type=d.get("type", "unknown"), params=dict(d.get("params") or {}))
        return cls.from_legacy_dict(d)

    @classmethod
    def from_legacy_dict(cls, d: dict) -> "StepConfig":
        t      = d.get("type", "unknown")
        params = {k: v for k, v in d.items() if k != "type"}
        return cls(type=t, params=params)


# ── ChainConfig ────────────────────────────────────────────────────────────────

@dataclass
class ChainConfig:
    chain_type: str
    steps:      list[StepConfig] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"chain_type": self.chain_type, "steps": [s.to_dict() for s in self.steps]}

    @classmethod
    def from_dict(cls, d: dict) -> "ChainConfig":
        return cls(
            chain_type = d.get("chain_type", ""),
            steps      = [StepConfig.from_dict(s) for s in d.get("steps", [])],
        )


# ── PipelineConfig ─────────────────────────────────────────────────────────────

@dataclass
class PipelineConfig:
    domain:            str
    fetch_chain:       ChainConfig = field(default_factory=lambda: ChainConfig("fetch"))
    extract_chain:     ChainConfig = field(default_factory=lambda: ChainConfig("extract"))
    title_chain:       ChainConfig = field(default_factory=lambda: ChainConfig("title"))
    nav_chain:         ChainConfig = field(default_factory=lambda: ChainConfig("navigate"))
    validate_chain:    ChainConfig = field(default_factory=lambda: ChainConfig("validate"))
    score:             float = 0.0
    optimizer_version: int   = 1
    created_at:        str   = ""
    notes:             str   = ""

    def to_dict(self) -> dict:
        return {
            "domain":            self.domain,
            "fetch_chain":       self.fetch_chain.to_dict(),
            "extract_chain":     self.extract_chain.to_dict(),
            "title_chain":       self.title_chain.to_dict(),
            "nav_chain":         self.nav_chain.to_dict(),
            "validate_chain":    self.validate_chain.to_dict(),
            "score":             self.score,
            "optimizer_version": self.optimizer_version,
            "created_at":        self.created_at,
            "notes":             self.notes,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineConfig":
        return cls(
            domain            = d.get("domain", ""),
            fetch_chain       = ChainConfig.from_dict(d.get("fetch_chain", {})),
            extract_chain     = ChainConfig.from_dict(d.get("extract_chain", {})),
            title_chain       = ChainConfig.from_dict(d.get("title_chain", {})),
            nav_chain         = ChainConfig.from_dict(d.get("nav_chain", {})),
            validate_chain    = ChainConfig.from_dict(d.get("validate_chain", {})),
            score             = float(d.get("score", 0.0)),
            optimizer_version = int(d.get("optimizer_version", 1)),
            created_at        = d.get("created_at", ""),
            notes             = d.get("notes", ""),
        )

    @classmethod
    def default_for_domain(cls, domain: str) -> "PipelineConfig":
        return cls(
            domain        = domain,
            fetch_chain   = ChainConfig("fetch", [StepConfig("hybrid"), StepConfig("playwright")]),
            extract_chain = ChainConfig("extract", [
                StepConfig("selector"),
                StepConfig("json_ld"),
                StepConfig("density_heuristic"),
                StepConfig("fallback_list"),
                StepConfig("ai_extract"),
            ]),
            title_chain = ChainConfig("title", [
                StepConfig("selector"),
                StepConfig("h1_tag"),
                StepConfig("title_tag"),
                StepConfig("og_title"),
                StepConfig("url_slug"),
            ]),
            nav_chain = ChainConfig("navigate", [
                StepConfig("rel_next"),
                StepConfig("selector"),
                StepConfig("anchor_text"),
                StepConfig("slug_increment"),
                StepConfig("fanfic"),
                StepConfig("ai_nav"),
            ]),
            validate_chain = ChainConfig("validate", [
                StepConfig("length",         {"min_chars": 100}),
                StepConfig("prose_richness", {"min_word_count": 20}),
            ]),
            notes = "default_pipeline",
        )