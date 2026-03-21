import re
from typing import List
from .models import OutlineNode

def parse_markdown_outline(md_text: str) -> List[OutlineNode]:
    """
    使用正则表达式和AST栈结构，准确解析大模型生成的带有页码和长摘要的 Markdown 树。
    """
    # 匹配 '# 标题 (页码: 12)' 
    # ^(#+)\s+ 匹配Markdown级别
    # (.*?)\s* 匹配标题内容
    # (?:\(页码:\s*(\d+)\))? 匹配可选的数字页码
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
            # 非标题行视为摘要内容追加
            if current_node and line.strip() != "":
                current_summary_lines.append(line)
                
    # 结算最后一块末尾数据
    if current_node:
        current_node.summary = "\n".join(current_summary_lines).strip()
        
    return root_nodes

def get_leaf_nodes(nodes: List[OutlineNode]) -> List[OutlineNode]:
    """
    递归提取所有的底层目录（叶子节点），它是步骤2进行深层增强处理的基本单元。
    """
    leafs = []
    def traverse(n: OutlineNode):
        if not n.children:
            leafs.append(n)
        for child in n.children:
            traverse(child)
    
    for root in nodes:
        traverse(root)
    return leafs
