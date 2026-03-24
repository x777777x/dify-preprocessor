from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class OutlineNode:
    level: int
    title: str
    page_num: str
    summary: str
    children: List['OutlineNode'] = field(default_factory=list)
    parent: Optional['OutlineNode'] = field(default=None, repr=False)
    
    @property
    def path(self) -> str:
        """获取整个文档目录层级路径，例如：一级标题>二级标题>三级标题"""
        if self.parent:
            return f"{self.parent.path}>{self.title}"
        return self.title

    @property
    def arrow_path(self) -> str:
        """获取指定排版样式的安全连接符路径：一级标题 ->> 二级标题 ->> 三级标题"""
        if self.parent:
            return f"{self.parent.arrow_path} ->> {self.title}"
        return self.title

    @property
    def level_titles(self) -> List[str]:
        """按层级顺序获取所有父级列表(专供Excel动态展开表头)。如 [一级标题名, 二级标题名, 三级标题名]"""
        titles = []
        curr = self
        while curr:
            titles.insert(0, curr.title)
            curr = curr.parent
        return titles

    @property
    def parent_context(self) -> str:
        """获取当前节点之上所有父级标题和深层摘要，组装为强大的全局上下文"""
        ctx = []
        curr = self.parent
        while curr:
            ctx.insert(0, f"【{curr.title}】\n层级深度摘要：{curr.summary}")
            curr = curr.parent
        return "\n\n".join(ctx)
