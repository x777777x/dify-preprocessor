import os
import sys
import argparse

try:
    import docx
except ImportError:
    print("请先安装依赖: pip install python-docx")
    sys.exit(1)

def split_word_by_lowest_heading(doc_path: str, output_path: str, target_level: int = None, separator: str = "##$$##$$##$$##$$##$$##$$##"):
    print(f"[*] 正在解析目标 Word 文档: {doc_path} ...")
    if target_level is not None:
        print(f"[*] 启用了强制分割层级覆盖模式，将全部对第 {target_level} 级大纲进行格式阻断。")
    
    # -------------------------------------------------------------
    # 第一步：彻底清洗和去除物理存在的文档目录（TOC）段落
    # -------------------------------------------------------------
    toc_paragraphs = []
    in_toc_section = False
    toc_level = 1
    
    for p in doc.paragraphs:
        style_name = p.style.name if p.style else ""
        
        # 1. 直接清洗原生自带 TOC 特式的引用段落
        if style_name.lower().startswith('toc'):
            toc_paragraphs.append(p)
            continue
            
        # 2. 靠锚定“目录”这种独立的大纲章段清洗隐藏目录
        title = p.text.replace(" ", "").lower()
        if style_name.startswith('Heading'):
            level_str = style_name.replace('Heading', '').strip()
            level = int(level_str) if level_str.isdigit() else 1
            
            if title in ["目录", "contents", "tableofcontents"]:
                in_toc_section = True
                toc_level = level
                toc_paragraphs.append(p)
                continue
            elif in_toc_section:
                # 遇到同级或更高级标题，意味着整个目录树的渲染区域结束
                if level <= toc_level:
                    in_toc_section = False

        if in_toc_section:
            toc_paragraphs.append(p)
            
    # 执行无痕静默物理删除
    count_deleted = 0
    for p in toc_paragraphs:
        try:
            p_element = p._element
            parent = p_element.getparent()
            if parent is not None:
                parent.remove(p_element)
                p._p = p_element = None
                count_deleted += 1
        except Exception:
            pass
            
    print(f"[*] 解析并成功排版清除了 {count_deleted} 条附带的独立目录格式数据块。")

    # -------------------------------------------------------------
    # 第二步：检索所有的底层叶子标题并插入特殊阻断字符串
    # -------------------------------------------------------------
    # 重新在清洗过的 DOM 中搜集所有大纲标题序列
    headings = []
    for p in doc.paragraphs:
        style_name = p.style.name if p.style else ""
        if style_name.startswith('Heading'):
            level_str = style_name.replace('Heading', '').strip()
            level = int(level_str) if level_str.isdigit() else 1
            headings.append((p, level))
            
    target_headings = []
    
    if target_level is not None:
        # 如果用户指派了具体大纲层级（比如 1 代表第一层级），无视父子关系纯暴力抽取该层的所有标题
        for curr_p, curr_level in headings:
            if curr_level == target_level:
                target_headings.append(curr_p)
    else:
        # 如果未指定层级，则走智能模式：寻找所有位于树底部、往下不再产生细分标题的真实物理叶子节点
        for i in range(len(headings)):
            curr_p, curr_level = headings[i]
            
            # 判定算法：若紧跟着它的下一个标题并没有陷得更深 (level <= curr_level)
            # 则说明当前标题没有任何子标题区域，也就是“最底层的大纲树标题 (叶子节点)”
            if i + 1 < len(headings):
                _, next_level = headings[i+1]
                if next_level <= curr_level:
                    target_headings.append(curr_p)
            else:
                # 整本书的最后一个章节自然一定是底层叶子节点
                target_headings.append(curr_p)
            
    if not target_headings:
        print("[!] 警告：未侦测到任何匹配的层级段落标题，原样输出。")
    
    # 向筛选出来的真正底层或指定节点头顶注入切割字符串
    for p in target_headings:
        # python-docx 的注入 API 可以确保原标题段落包含样式、内嵌在表格周围的图文元素绝不发生内存上的改动，且位置极其精准
        p.insert_paragraph_before(separator)
        
    print(f"[*] 成功识别出 {len(target_headings)} 个符合条件的独立知识块结构，并向其头顶注入隔离阻断符。")

    # -------------------------------------------------------------
    # 第三步：极速落盘（由于是增量改动内存树，天然保留了所有的格式、原位表格与复杂图片组）
    # -------------------------------------------------------------
    doc.save(output_path)
    print(f"[v] 全部任务执行完毕！保留绝对完美的图片、样式格式分割成果已输出至: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="按指定层级结构或最底层插入标识符，并清除原生目录区的 Word 切割排版工具")
    parser.add_argument("input", help="要处理的原始 Word 文件路径 (.docx)")
    parser.add_argument("-o", "--output", help="输出阻断重组后的 Word 文件存放路径 (可选)")
    parser.add_argument("-l", "--level", type=int, default=None, help="覆盖智能检测：直接选择在哪一层级标题上面加特殊的间隔符（例如指定1、2或3，留空则自动算最底层）")
    parser.add_argument("-s", "--separator", type=str, default="##$$##$$##$$##$$##$$##$$##", help="自定义要插在这里作为独立切割阻断的特殊字符串")
    
    args = parser.parse_args()
    
    input_path = args.input
    output_path = args.output
    target_lvl = args.level
    sep_str = args.separator
    
    if not output_path:
        base_name = os.path.splitext(input_path)[0]
        ext_suffix = f"_指定第{target_lvl}层_阻断版" if target_lvl else "_最底层_阻断版"
        output_path = f"{base_name}{ext_suffix}.docx"
        
    split_word_by_lowest_heading(input_path, output_path, target_level=target_lvl, separator=sep_str)
