import os
import sys
import argparse

try:
    import docx
    import pandas as pd
    from docx.document import Document
    from docx.oxml.text.paragraph import CT_P
    from docx.oxml.table import CT_Tbl
    from docx.table import _Cell, Table
    from docx.text.paragraph import Paragraph
except ImportError:
    print("请先安装依赖: pip install python-docx pandas openpyxl")
    sys.exit(1)

def iter_block_items(parent):
    """
    按文档物理顺序遍历所有的段落（Paragraph）和表格（Table）。
    保证内容提取顺序正确且不会遗漏表格内容。
    """
    if isinstance(parent, Document):
        parent_elm = parent.element.body
    elif isinstance(parent, _Cell):
        parent_elm = parent._tc
    else:
        raise ValueError("暂不支持遍历的父级结构类型")

    for child in parent_elm.iterchildren():
        if isinstance(child, CT_P):
            yield Paragraph(child, parent)
        elif isinstance(child, CT_Tbl):
            yield Table(child, parent)

def get_chinese_number(num: int) -> str:
    """将数字转换成中文用于表头（最高支持简单的一到十）"""
    mapping = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九', '十']
    if 1 <= num <= 10:
        return mapping[num]
    return str(num)

def convert_word_to_excel(doc_path: str, output_excel: str):
    print(f"[*] 正在解析 Word 文档: {doc_path} ...")
    doc = docx.Document(doc_path)
    
    records = []
    
    current_path = [] # 追踪当前的各级大纲路径
    current_content = []
    current_page = 1
    paragraph_count = 0
    
    def flush_content():
        """将当前堆积的段落清空并并入最后锚定的路径节点中"""
        if current_content and current_path:
            text = "\n".join(current_content).strip()
            if text:
                records.append({
                    "path": current_path.copy(),
                    "content": text,
                    "page": current_page
                })
        current_content.clear()

    for block in iter_block_items(doc):
        # 简单模拟虚拟页码机制（每处理20排文字算作一页）
        virtual_page = (paragraph_count // 20) + 1
        
        if isinstance(block, Paragraph):
            paragraph_count += 1
            style_name = block.style.name if block.style else ""
            
            if style_name.startswith('Heading'):
                # 遇到全新标题，立刻结算并保存之前挂载在其上方的散落正文
                flush_content()
                
                # 计算标题层级
                level_str = style_name.replace('Heading', '').strip()
                level = int(level_str) if level_str.isdigit() else 1
                title = block.text.strip()
                
                if not title:
                    continue
                    
                # 更新当前大纲路径层级
                if level > len(current_path):
                    # 如果发生跳级（比如出现 H1 后直接写了个 H3），中间补空处理防越界
                    while len(current_path) < level - 1:
                        current_path.append("")
                    current_path.append(title)
                else:
                    # 如果层级平级或回退，裁剪数组并追加最新标题
                    current_path = current_path[:level-1]
                    current_path.append(title)
                    
                current_page = virtual_page
            else:
                text = block.text.strip()
                if text:
                    # 如果文章一上来连任何 `# 标题` 都没有就开始写段落，强行开辟一个根节点
                    if not current_path:
                        current_path = ["文档前言"]
                    current_content.append(text)
                    
        elif isinstance(block, Table):
            # 将表格转换为类 Markdown 内容塞入正文以保持信息高度完整
            table_text = []
            for row in block.rows:
                # 提取每格内容并去除换行防止乱入同一行
                row_data = [cell.text.strip().replace('\n', ' ') for cell in row.cells]
                table_text.append(" | ".join(row_data))
                
            if table_text:
                if not current_path:
                    current_path = ["文档前言"]
                current_content.append("\n" + "\n".join(table_text) + "\n")
                
    # 强制把最后一章结尾堆积的内容强制刷入
    flush_content()
    
    if not records:
        print("[!] 提取失败，未能提取到任何有效内容。")
        return
        
    print(f"[*] 段落与表格挂载归并完毕。侦测到最深化结构层级数：{max(len(r['path']) for r in records)}")
    
    max_level = max(len(r["path"]) for r in records)
    
    rows = []
    for r in records:
        row = {}
        for lvl in range(max_level):
            col_name = f"第{get_chinese_number(lvl+1)}级标题"
            row[col_name] = r["path"][lvl] if lvl < len(r["path"]) else ""
            
        row["页码"] = str(r["page"])
        row["最底层标题中的内容"] = r["content"]
        rows.append(row)
        
    df = pd.DataFrame(rows)
    df.to_excel(output_excel, index=False)
    print(f"[v] 已成功将层级分离的规整数据导出至: {output_excel}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="按底层目录路径分割并展平 Word 的内容至 Excel")
    parser.add_argument("input", help="要处理的原始 Word 文件路径 (.docx)")
    parser.add_argument("-o", "--output", help="输出的 Excel 文件存放路径 (可选)")
    
    args = parser.parse_args()
    
    input_path = args.input
    output_path = args.output
    
    if not output_path:
        base_name = os.path.splitext(input_path)[0]
        output_path = f"{base_name}_层级切分版.xlsx"
        
    convert_word_to_excel(input_path, output_path)
