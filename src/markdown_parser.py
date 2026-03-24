import re
from typing import List
from .models import OutlineNode

def parse_markdown_outline(md_text: str) -> List[OutlineNode]:
    """
    使用正则表达式和AST栈结构，准确解析大模型生成的带有页码和长摘要的 Markdown 树。
    """
    # 匹配 '# 标题', 去除以前行内页码的强校验
    header_pattern = re.compile(r'^(#+)\s+(.*?)(?:\s*\(页码:\s*(\d+)\))?\s*$')
    lines = md_text.split('\n')
    
    root_nodes = []
    stack = [] # 栈中保存 (OutlineNode, level)
    
    current_node = None
    current_summary_lines = []
    
    for line in lines:
        match = header_pattern.match(line)
        if match:
            # 当遇到新标题时，结算并保存上一标题挂载的 300-500 字深层摘要
            if current_node:
                current_node.summary = "\n".join(current_summary_lines).strip()
            
            level = len(match.group(1))
            title = match.group(2).strip()
            page_num = match.group(3) if match.group(3) else "未知"
            
            new_node = OutlineNode(level=level, title=title, page_num=page_num, summary="")
            
            # 回溯栈，寻找正确的直接父节点
            while stack and stack[-1][1] >= level:
                stack.pop()
                
            if stack:
                parent = stack[-1][0]
                new_node.parent = parent
                parent.children.append(new_node)
            else:
                root_nodes.append(new_node)
                
            stack.append((new_node, level))
            current_node = new_node
            current_summary_lines = []
        else:
            # 非标题行可能是换行后的单独页码，或者是摘要内容
            if current_node and line.strip() != "":
                # 匹配新型的换行页码格式：^页码 10$
                page_match = re.match(r'^页码\s*(\d+)$', line.strip())
                if page_match and current_node.page_num == "未知":
                    current_node.page_num = page_match.group(1)
                else:
                    current_summary_lines.append(line)
                
    # 结算最后一块末尾数据
    if current_node:
        current_node.summary = "\n".join(current_summary_lines).strip()
        
    return root_nodes

def get_leaf_nodes(nodes: List[OutlineNode]) -> List[OutlineNode]:
    """
    递归提取所有的底层目录（叶子节点），它是步骤2进行深层增强处理的基本单元。
    考虑到第一步可能因为滑动窗口重叠生成重复的相同层级，此处基于 path 进行去重。
    """
    leafs = []
    seen_paths = set()
    
    def traverse(n: OutlineNode):
        if not n.children:
            # 去除首尾不可见字符的干净路径作为唯一键
            clean_path = n.path.strip()
            if clean_path not in seen_paths:
                seen_paths.add(clean_path)
                leafs.append(n)
        else:
            for child in n.children:
                traverse(child)
    
    for root in nodes:
        traverse(root)
    return leafs
