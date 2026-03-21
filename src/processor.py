import os
import sys
import pandas as pd
import logging
import time
import hashlib
from typing import List, Dict, Tuple
from tqdm import tqdm

from .document_parser import extract_text_with_pages
from .markdown_parser import parse_markdown_outline, get_leaf_nodes
from .llm_client import call_llm
from .prompts import STEP1_SYSTEM_PROMPT, STEP2_SYSTEM_PROMPT, STEP3_SYSTEM_PROMPT, STEP4_SYSTEM_PROMPT

# 导入外部健壮性配置
import config
MAX_RETRIES = getattr(config, "MAX_RETRIES", 3)
INTERMEDIATE_DIR = getattr(config, "INTERMEDIATE_DIR", "./dify_intermediate")

def setup_logger(output_dir: str):
    logger = logging.getLogger("DifyPreProcessor")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        os.makedirs(output_dir, exist_ok=True)
        log_file = os.path.join(output_dir, "processor_execution.log")
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_format = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        file_handler.setFormatter(file_format)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_format = logging.Formatter('%(asctime)s - %(message)s', datefmt="%H:%M:%S")
        console_handler.setFormatter(console_format)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger

class DifyPreProcessor:
    def __init__(self, file_path: str, output_dir: str):
        self.file_path = file_path
        self.output_dir = output_dir
        self.logger = setup_logger(self.output_dir)
        
        # 确保全局的容灾中间产物保护目录存在
        os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
        
        # 1. 计算文件分析维度的前缀：时间戳 + 文件名MD5(前8位)
        filename = os.path.basename(self.file_path)
        self.timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        self.md5_str = hashlib.md5(filename.encode('utf-8')).hexdigest()[:8]
        self.run_prefix = f"{self.timestamp_str}_{self.md5_str}_"
        
        # 2. 建立或追加维护中间件索引记录文件
        index_csv_path = os.path.join(INTERMEDIATE_DIR, "file_index.csv")
        file_exists = os.path.exists(index_csv_path)
        with open(index_csv_path, "a", encoding="utf-8-sig") as f:
            if not file_exists:
                f.write("时间戳,MD5前8位,原始文件名\n")
            f.write(f"{self.timestamp_str},{self.md5_str},{filename}\n")

    def run(self):
        self.logger.info(f"========== 解析任务启动 ==========")
        self.logger.info(f"[*] 侦测目标文档: {self.file_path}")
        self.logger.info(f"[*] 缓存重试次数设定: {MAX_RETRIES} 次")
        self.logger.info(f"[*] 中间文件持久化保存路径: {INTERMEDIATE_DIR}")
        
        try:
            pages = extract_text_with_pages(self.file_path)
            full_text = "\n".join([f"【第{p['page_num']}页】\n{p['text']}" for p in pages])
            self.logger.info(f"[v] 文档解析引擎工作正常，共识别提取了 {len(pages)} 个页块容纳。")
        except Exception as e:
            self.logger.error(f"[x] 文档解析直接发生错误: {e}", exc_info=True)
            return

        self.logger.info("[*] [步骤 1] 正在发起全局 Markdown 抽取的 Map 线程池...")
        
        chunk_size = 5
        outline_md_parts = []
        
        pbar_s1 = tqdm(range(0, len(pages), chunk_size), desc="分块大纲提取管道", unit="区块", dynamic_ncols=True)
        for i in pbar_s1:
            chunk_pages = pages[i : i + chunk_size]
            chunk_text = "\n".join([f"【第{p['page_num']}页】\n{p['text']}" for p in chunk_pages])
            pbar_s1.set_description(f"范围页码: {i+1}-{min(i+chunk_size, len(pages))}")
            
            chunk_prompt = f"以下是一份大卷白皮书的局部第 {i+1} 至 {min(i+chunk_size, len(pages))} 页碎片片段。请严格抽取这当中的 Markdown 大纲结构(含页码及深度摘要)：\n\n{chunk_text}"
            
            # --- 重载失败恢复重试与存盘机制 (Step 1) ---
            chunk_success = False
            for attempt in range(MAX_RETRIES):
                chunk_md = call_llm(STEP1_SYSTEM_PROMPT, chunk_prompt, temperature=0.2)
                if chunk_md:
                    # 并发性结果保存为本地单独文件
                    inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step1_batch_{i}_to_{min(i+chunk_size, len(pages))}.md")
                    with open(inter_file, "w", encoding="utf-8") as f:
                        f.write(chunk_md)
                        
                    outline_md_parts.append(chunk_md)
                    chunk_success = True
                    break
                else:
                    self.logger.warning(f"    [!] 网络/响应为空，由于配置了容灾机制，正在调度重试 {attempt+1}/{MAX_RETRIES} (Batch {i})...")
            
            if not chunk_success:
                self.logger.error(f"    [x] 这个局部页面块 {i+1}至{min(i+chunk_size, len(pages))} 号彻底丢包失败，放弃解析以防阻塞。")
                
        outline_md = "\n\n".join(outline_md_parts)
        
        # 本地快照整个全局大纲树结构结果
        with open(os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step1_Full_Rebuilt_Outline.md"), "w", encoding="utf-8") as f:
            f.write(outline_md)
            
        if not outline_md.strip():
            self.logger.error("[x] 整个第一步循环最终未能沉淀任何目录树格式数据，发生核心断桥级故障！")
            return
            
        self.logger.info("[v] 第一步滑动窗口切片大纲完成！全局 AST 结构树解析即将加载...")
        root_nodes = parse_markdown_outline(outline_md)
        leaf_nodes = get_leaf_nodes(root_nodes)
        
        if not leaf_nodes:
            self.logger.warning("[!] 树干被系统发现但丢失了任何具有细分知识意义的分支/底层，生成树似乎被幻觉带偏了格式。")
            return
            
        self.logger.info(f"[*] 解析结束。统计具备增强价值的末端独立模块 {len(leaf_nodes)} 个，开始分发到强化审核工厂。")
        
        final_data = []
        pbar = tqdm(leaf_nodes, desc="知识审核增强进度", unit="底层原子节点", dynamic_ncols=True)
        
        for i, node in enumerate(pbar):
            node_name_short = node.title[:15] + ".." if len(node.title) > 15 else node.title
            pbar.set_description(f"处理挂载: {node_name_short}")
            
            self.logger.info(f"\n-----------------------------------------------------------")
            self.logger.info(f"[*] 提取第 {i+1}/{len(leaf_nodes)} 支点上下文 | AST系统绝对路径: [{node.path}]")
            
            original_content = "\n".join([p['text'] for p in pages if str(p['page_num']) == node.page_num])
            if not original_content.strip():
                self.logger.warning(f"    [!] 物理文本页码越界映射！系统将用自身的单段底层摘要回退覆盖其缺失。")
                original_content = node.summary

            self.logger.info(f"    -> [步骤 2] 请求主大语言引擎分配针对节点背景知识深层报告...")
            step2_user_prompt = f"【全局背景树（包含父级与摘要）】:\n{node.parent_context}\n\n【叶子节点极度关联的原始片段】:\n{original_content}"
            
            # --- 强化重试机制与落盘 (Step 2) ---
            content_report = None
            for attempt in range(MAX_RETRIES):
                content_report = call_llm(STEP2_SYSTEM_PROMPT, step2_user_prompt, temperature=0.3)
                if content_report:
                    inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step2_node_{i+1}_raw_report.md")
                    with open(inter_file, "w", encoding="utf-8") as f:
                        f.write(content_report)
                    break
                else:
                    self.logger.warning(f"    [!] OOM/网络丢失中断模型生成，已配置接管，重试 {attempt+1}/{MAX_RETRIES}...")
                    
            if not content_report:
                self.logger.error(f"    [x] 单体增强 {node.title} 彻底重创，系统不得不弃置此点。")
                continue

            self.logger.info(f"    -> [步骤 3] 数据流动到第二台 LLM 引擎负责质检审核以及锚点标签标定...")
            step3_user_prompt = f"【原始片段源数据】:\n{original_content}\n\n【步骤二生成的初版内容报告】:\n{content_report}"
            
            # --- 强化重试机制与落盘 (Step 3) ---
            reviewed_res = None
            keywords, revised_report = "", ""
            for attempt in range(MAX_RETRIES):
                reviewed_res = call_llm(STEP3_SYSTEM_PROMPT, step3_user_prompt, temperature=0.1) 
                if reviewed_res:
                    inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step3_node_{i+1}_final_reviewed.md")
                    with open(inter_file, "w", encoding="utf-8") as f:
                        f.write(reviewed_res)
                        
                    keys, revised = self._parse_review_response(reviewed_res)
                    if "未能提取" not in keys:
                        keywords, revised_report = keys, revised
                        break
                    else:
                        self.logger.warning(f"    [!] 审核模型输出内容由于没有正确带上 '---关键词---' 分割符报错，重写纠正 {attempt+1}/{MAX_RETRIES}...")
                else:
                    self.logger.warning(f"    [!] 审核模型引擎连接崩溃或空出，请求重连 {attempt+1}/{MAX_RETRIES}...")
            
            if not reviewed_res or not keywords:
                self.logger.error("    [x] 针对节点审计强弱检验失败！系统判定启用紧急降级接管策略：使用未审计的 Step 2 版报告。")
                keywords, revised_report = "无结构化规范词", content_report
                
            self.logger.info(f"    [v] 锚点(检索关键词)确认提炼成功: {keywords[:20]}...")
            
            record_info = {}
            for lvl_idx, title in enumerate(node.level_titles):
                record_info[f"{lvl_idx+1}级标题"] = title
                
            record_info.update({
                "底层节点摘要": node.summary if node.summary else "无摘要",
                "对应页码": node.page_num,
                "核心关键词": keywords,
                "完整增强内容": revised_report
            })
            
            final_data.append(record_info)

            safe_title = "".join([c for c in node.title if c.isalpha() or c.isdigit() or c in ' -_']).strip()
            markdown_path = os.path.join(self.output_dir, f"Node_{i+1}_{safe_title}.md")
            self._write_markdown_chunks(node.path, record_info, markdown_path)
            self.logger.info(f"    [v] 属于 {node.title} 的所有多格式形态数据落盘完全存取至: {markdown_path}")

        self.logger.info(f"\n[*] [步骤 4] 所有碎片全部审计通过，触发最后的一体化报表终态变换引擎...")
        self._export_to_excel(final_data)
        self.logger.info("========== Dify-PreProcessor 流畅跨越全部高危障碍线，顺利着陆！ ==========")

    def _parse_review_response(self, text: str) -> Tuple[str, str]:
        if '---关键词---' in text and '---修正后的内容报告---' in text:
            parts = text.split('---修正后的内容报告---')
            keys = parts[0].replace('---关键词---', '').strip()
            revised = parts[1].strip()
            return keys, revised
        return "未能提取准确关键词", text 
        
    def _write_markdown_chunks(self, path_str: str, data: Dict, file_path: str):
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# 路径体系：{path_str}\n\n")
            f.write(f"**溯源极简页码**: {data.get('对应页码', '')}\n\n")
            f.write(f"**Dify高频检索标记**: {data.get('核心关键词', '')}\n\n")
            f.write(f"## 底层片段摘要\n{data.get('底层节点摘要', '')}\n\n")
            f.write(f"---\n\n## 高质量增强正文内容\n\n{data.get('完整增强内容', '')}\n")

    def _export_to_excel(self, final_data: List[Dict]):
        if not final_data:
            self.logger.warning("[!] 当前批流式没有收集到任何合法载入字典的表报数据，终止渲染操作。")
            return
            
        self.logger.info("    -> [步骤 4] 请求大语言模型转换此批字典内存数组至严谨的 RFC 4180 CSV 字符串...")
        import json, io
        
        json_dump = json.dumps(final_data, ensure_ascii=False)
        step4_prompt = f"请严格将下列 JSON 转换为没有任何 Markdown 包装代码块的纯净的 CSV 文本:\n\n{json_dump}"
        
        df = None
        # --- 重用防抖落盘机制 (Step 4 CSV) ---
        for attempt in range(MAX_RETRIES):
            csv_text = call_llm(STEP4_SYSTEM_PROMPT, step4_prompt, temperature=0.1)
            
            if csv_text:
                if csv_text.strip().startswith("```"):
                    csv_text = "\n".join(csv_text.strip().split("\n")[1:-1])
                    
                inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step4_LLM_Bare_Naked.csv")
                with open(inter_file, "w", encoding="utf-8") as f:
                    f.write(csv_text)
                    
                self.logger.info("    [v] 获取大模型生成的裸 CSV 数据完成！立刻将控制权转交给底座 Pandas 验证读入能力...")
                try:
                    df = pd.read_csv(io.StringIO(csv_text))
                    break 
                except Exception as e:
                    self.logger.warning(f"    [!] LLM 的确给了 CSV 但解析崩盘带出 {e}。执行 Pandas 回溯校验或者再生成 {attempt+1}/{MAX_RETRIES}...")
            else:
                 self.logger.warning(f"    [!] 模型因为超时未能送达流信息包，正在复活重拉 {attempt+1}/{MAX_RETRIES}...")

        if df is None:
            self.logger.error("    [x] 步骤四格式转换被抛出三次以上重写报错阻断！最终兜底回切 Pandas 一键 `pd.DataFrame` Json源构建。")
            df = pd.DataFrame(final_data)
        
        title_cols = [c for c in df.columns if '级标题' in c]
        title_cols = sorted(title_cols, key=lambda x: int(x.split('级')[0]))
        
        other_cols = ["底层节点摘要", "对应页码", "核心关键词", "完整增强内容"]
        final_cols = [c for c in title_cols + other_cols if c in df.columns]
        
        df = df.reindex(columns=final_cols)
        
        excel_path = os.path.join(self.output_dir, "Dify_知识库增强底座.xlsx")
        df.to_excel(excel_path, index=False)
        self.logger.info(f"    [v] 构建物理结构最终映射成功，Excel 数据舱落盘位于: {excel_path}")
