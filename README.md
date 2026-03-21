# Dify-PreProcessor

**Dify-PreProcessor** 是一个高度智能化、专为应对“长文本喂给本地模型 OOM”痛点而生的数据前置增强清洗渠道。

它可以将你的本地 PDF 或 Word 巨型长卷文档，通过精妙的 **局部滑动窗口 (Map-Reduce)** 技术和强制提示工程，进行逐段理解、提炼，最终重组为带有强大层级上下文、不易丢失原始语义的纯粹 CSV 与结构化 Excel 表单。一键无缝接入并增强 Dify 等知识库应用！

---

## 🌟 核心特性概览

* **极光滑动窗口 Map-Reduce**：全文拆散处理，完美避开了本地局域网如 Ollama `Qwen3.5:35B` 等超大参量模型一波流塞入长文本引发的 `502 Bad Gateway` (VRAM/内存溢出) 惨案。
* **高鲁棒性容灾机制**：任何一次 API 调用断联、模型由于幻觉没吐出所需格式，系统会自动拦截报错并进行 `MAX_RETRIES` 的原地修正重拉，永不轻易断桥。
* **全生命周期防丢快照 (MD5 溯源)**：分析起手会计算当前时间的绝对戳以及当前文件专属的 MD5 散列名。从拆块大纲、每级知识增强，到提取审核节点，全部携带指纹前缀落盘于 `INTERMEDIATE_DIR` 中。
* **终端实时打字机推流**：告别干等！即使大模型慢如牛车，脚本内置了标准 Stream 流式直推，以及酷炫的 `tqdm` 原生双进度条。
* **动态 N 级表头 Excel**：依据模型抽离的 AST 树层级深度，自适应拉平并延展表头列（一级标题、二级标题...核心关键词、底层摘要等），强迫症狂喜的 Excel 输出。

---

## 📂 项目结构

```text
e:/project/dify-preprocessor/
├── src/
│   ├── document_parser.py  # (PDFPlumber/Docx 精准抽字与物理映射)
│   ├── markdown_parser.py  # (正则与基于栈的 AST 抽象语法树标题缝合器)
│   ├── llm_client.py       # (对齐 OpenAI API 范式的终端流式打字机)
│   ├── processor.py        # (四段式增强链路指挥官，统筹全局管线)
│   └── prompts.py          # (极度严苛、规避幻觉的系统级 Prompts 矩阵)
├── config.py               # (独立配置文件：地址、模型、防抖策略)
├── requirements.txt        # (包含 openpyxl, pdfplumber, pandas, tqdm 等)
├── main.py                 # (启动阀)
└── .gitignore 
```

---

## 🚀 工作流 (The Pipeline)

程序底层严格执行不可逆的四大工序：
1. **工序一（提炼）**：以 5 页为单位分块请求 LLM，抽离所有合法带有 `(页码: X)` 以及 `300-500` 字大段摘要的 `#` 号大纲，利用 AST 组装缝合为整树。
2. **工序二（增强）**：向下挖掘所有的“树叶（最底层目录）”，叠加上它的全体父系标题背景，要求 LLM 一对一产出没有孤立歧义的“重构报告”。
3. **工序三（质检）**：启动质检 LLM 专员，查杀幻觉，压榨提取有利于 Dify 倒排/向量检索的 5-10 个精选 `核心标签 (Keywords)`。
4. **工序四（落卷）**：再次驱动最严苛的格式 LLM 专员，把 Python 内存组装好的所有节点 JSON 暴力清洗为标准的纯文本 CSV，再交给 Pandas 安全封装为 `xlsx`。

---

## ⚙️ 快速上手

1. **环境准备**：
   确保你安装了 Python 3.9+，建议在虚拟环境中使用：
   ```bash
   pip install -r requirements.txt
   ```

2. **核心配置 (`config.py`)**：
   在根目录下修改属于你自己的模型配置：
   ```python
   # Ollama 或其他 OpenAI 兼容的 API 入口
   OPENAI_API_BASE = "http://127.0.0.1:11434/v1"
   OPENAI_API_KEY = "ollama"

   # 模型选用
   LLM_MODEL = "qwen3.5:35b-a3b"
   
   MAX_RETRIES = 3 # 容灾重试次数
   ```

3. **放置数据跑批**：
   打开 `main.py`，将 `docs_path` 变量指向你需要解构的超级大部头 PDF 或是长长的法务 `DOCX` 文书。
   ```bash
   python main.py
   ```

4. **验收资产**：
   - 最终成果库：`dify_processed_output/Dify_知识库增强底座.xlsx`
   - 调试与追踪快照库：`dify_intermediate/` （一切包含时间戳与 MD5 指纹的原始思维断点全在这个文件夹）

---
*Happy Processing!* 
