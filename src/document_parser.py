import pdfplumber
from typing import List, Dict

try:
    import docx
except ImportError:
    docx = None

def extract_text_with_pages(file_path: str) -> List[Dict[str, str]]:
    """
    通过 pdfplumber 或 python-docx 提取文档内容并带有原始页码标记
    返回结构: [{"page_num": "1", "text": "第一页原文内容"}, ...]
    """
    ext = file_path.lower().split('.')[-1]
    if ext == 'pdf':
        return _parse_pdf(file_path)
    elif ext in ['doc', 'docx']:
        return _parse_docx(file_path)
    else:
        raise ValueError(f"暂不支持解析的文件格式后缀: {ext}")

def _parse_pdf(file_path: str) -> List[Dict[str, str]]:
    pages_data = []
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text and text.strip():
                pages_data.append({
                    "page_num": str(i + 1),
                    "text": text.strip()
                })
    return pages_data

def _parse_docx(file_path: str) -> List[Dict[str, str]]:
    if not docx:
        raise ImportError("请安装 python-docx 库以解析 Word 文档")
    doc = docx.Document(file_path)
    
    # 简单实现：将 Docx 合并处理，因无严格物理页码，可按一定段落数量分块挂接虚拟页码
    # 此处暂定以每 20 个段落视为 1 个"虚拟页码"片段
    pages_data = []
    current_page = 1
    current_text = []
    
    for i, p in enumerate(doc.paragraphs):
        if p.text.strip():
            current_text.append(p.text.strip())
        if (i > 0 and i % 20 == 0) or i == len(doc.paragraphs) - 1:
            if current_text:
                pages_data.append({
                    "page_num": str(current_page),
                    "text": "\n".join(current_text)
                })
                current_page += 1
                current_text = []
                
    return pages_data
