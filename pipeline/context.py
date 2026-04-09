"""
pipeline/context.py — Factory và helpers cho PipelineContext.

P3-C: Giải thích lý do file này tồn tại riêng thay vì merge vào base.py.

Lý do tách ra khỏi base.py:
    base.py định nghĩa abstract types (ScraperBlock, BlockResult, StepConfig,
    PipelineConfig, v.v.) và không nên import BeautifulSoup hay bất kỳ
    heavy dependency nào — base.py được import ở rất nhiều chỗ, thêm
    BeautifulSoup import vào đó sẽ tăng overhead cho mọi module chỉ cần types.

    context.py import PipelineContext từ base.py và add factory logic trên đó.
    Caller (executor.py, scraper.py) chỉ cần import từ context.py,
    không cần biết internals của PipelineContext.

    Circular import prevention:
    base.py ← pipeline/context.py ← pipeline/executor.py
    Nếu merge context.py vào base.py, executor.py sẽ phải import từ base.py
    mà base.py lại cần executor.py types → circular.
"""
from __future__ import annotations

from pipeline.base import PipelineContext


def make_context(
    url     : str,
    profile : dict | None = None,
    progress: dict | None = None,
) -> PipelineContext:
    """
    Factory function tạo PipelineContext mới cho một chapter.

    Args:
        url:      URL của chapter cần scrape
        profile:  SiteProfile dict (từ profile_manager)
        progress: ProgressDict (từ progress file)

    Returns:
        PipelineContext mới, sẵn sàng cho pipeline execution
    """
    return PipelineContext(
        url      = url,
        profile  = profile  or {},
        progress = progress or {},
    )


def context_summary(ctx: PipelineContext) -> str:
    """
    Tóm tắt kết quả context sau khi pipeline chạy xong.
    Dùng để log / debug.
    """
    parts = [f"[{ctx.url[:55]}]"]

    if ctx.html:
        parts.append(f"html={len(ctx.html):,}c")
    if ctx.content:
        parts.append(f"content={len(ctx.content):,}c")
    if ctx.title_clean:
        parts.append(f"title={ctx.title_clean[:30]!r}")
    if ctx.next_url:
        parts.append(f"next=✓")
    else:
        parts.append("next=✗")

    score = ctx.get_pipeline_score()
    parts.append(f"score={score['total']:.2f}")

    if ctx.errors:
        parts.append(f"errors={len(ctx.errors)}")

    return " | ".join(parts)