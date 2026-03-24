import os
import sys
import pandas as pd
import logging
import time
import hashlib
from typing import List, Dict, Tuple
from tqdm import tqdm
import re

from .document_parser import extract_text_with_pages, extract_native_toc
from .markdown_parser import parse_markdown_outline, get_leaf_nodes
from .llm_client import call_llm
from .prompts import STEP1_SYSTEM_PROMPT, STEP2_SYSTEM_PROMPT, STEP3_SYSTEM_PROMPT, STEP4_SYSTEM_PROMPT, TOC_EXTRACT_SYSTEM_PROMPT, STEP1_WITH_TOC_SYSTEM_PROMPT

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

    def _extract_global_toc(self, pages: List[Dict]) -> Tuple[str, int]:
        self.logger.info("[*] [步骤 0] 启动全局物理目录提取与规整阶段...")
        current_pages_to_check = 10
        max_pages_limit = 50
        
        while current_pages_to_check <= max_pages_limit:
            end_idx = min(len(pages), current_pages_to_check)
            chunk_text = "\n".join([f"【第{p['page_num']}页】\n{p['text']}" for p in pages[:end_idx]])
            
            prompt = f"请分析以下这 {end_idx} 页的开篇文本内容：\n\n{chunk_text}"
            
            res = None
            for attempt in range(MAX_RETRIES):
                res = call_llm(TOC_EXTRACT_SYSTEM_PROMPT, prompt, temperature=0.1)
                if res:
                    break
                else:
                    self.logger.warning(f"    [!] 网络/响应为空，调度重试 {attempt+1}/{MAX_RETRIES}...")
            
            if not res:
                self.logger.error("    [x] 目录探测完全失败，回退无目录降级模式。")
                return "", 0
                
            res_stripped = res.strip()
            
            if "[NO_TOC]" in res_stripped:
                self.logger.info("    [v] 该文档并未包含可用的物理原生目录，开启自主解析切片模式。")
                return "", 0
                
            if "[TOC_INCOMPLETE]" in res_stripped:
                # 【方法二：页码判断法兜底】
                if len(pages) <= current_pages_to_check:
                     self.logger.warning("    [!] 材料实际总页数已耗尽，正文未能被确认开始或探测触底，强制放弃延伸直接降级模式！")
                     return "", 0
                
                self.logger.info(f"    [!] 判定依据触发：正文未开始或目录末尾页码小于底线，发送 1-{current_pages_to_check + 10} 页动态扩大视野...")
                current_pages_to_check += 10
                continue
                
            if "[TOC_COMPLETE]" in res_stripped:
                self.logger.info(f"    [v] 在前 {end_idx} 页内成功抓取到全书原生目录树！正在清理标记并装载为全局真理基座。")
                
                # 剥离判断头部标志，提取纯粹的目录正文和起始页码
                parts = res_stripped.split("[TOC_COMPLETE]")
                extracted_toc_raw = parts[-1].strip() if len(parts) > 1 else res_stripped
                
                start_page_idx = 0
                match = re.search(r'^正文物理起始页\s+(\d+)\s*$', extracted_toc_raw, re.MULTILINE)
                if match:
                    start_page_str = match.group(1)
                    extracted_toc = re.sub(r'^正文物理起始页\s+\d+\s*$', '', extracted_toc_raw, flags=re.MULTILINE).strip()
                    for idx, p in enumerate(pages):
                        if str(p['page_num']) == start_page_str:
                            start_page_idx = idx
                            break
                else:
                    extracted_toc = extracted_toc_raw.strip()
                
                if extracted_toc:
                    inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step0_Global_TOC.md")
                    with open(inter_file, "w", encoding="utf-8") as f:
                        f.write(extracted_toc)
                    return extracted_toc, start_page_idx
                else:
                    self.logger.warning("    [!] 模型虽然输出了 `[TOC_COMPLETE]` 标志但未输出后面的具体目录实体，放弃提取直接降级！")
                    return "", 0
                    
            # 兼容未包含任何标志位的异常裸给目录情况
            self.logger.warning(f"    [!] 模型未严格遵循标志位要求，尝试暴力拉取当前结果直接落盘。")
            inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step0_Global_TOC.md")
            with open(inter_file, "w", encoding="utf-8") as f:
                f.write(res_stripped)
            return res_stripped, 0
            
        self.logger.warning(f"    [!] 目录查找深度超过最大限制 {max_pages_limit} 页，强制终止，直接降级。")
        return "", 0

    def _run_step0_toc_extraction(self, pages: List[Dict]) -> Tuple[str, int]:
        """执行步骤零：原生物理探针抓取目录与大模型 fallback 双重解析引擎"""
        self.logger.info("[*] [首层漏斗] 启动原生文档结构代码探针检测...")
        global_toc_md, start_idx = extract_native_toc(self.file_path)

        if global_toc_md:
            self.logger.info("    [v] BINGO! 瞬间捕获内嵌数字书签目录树，彻底跳过大模型探测盲盒开销，直接装载为最高真理基座。")
            inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step0_Native_TOC.md")
            with open(inter_file, "w", encoding="utf-8") as f:
                f.write(global_toc_md)
        else:
            self.logger.info("    [!] 原生书签失效或文档已被物理抹平，降级启动大模型内容提取测绘漏斗...")
            global_toc_md, start_idx = self._extract_global_toc(pages)

        if start_idx > 0:
             self.logger.info(f"[*] 检测到目录长度，正文实际始于第 {start_idx + 1} 个分页块。系统将完全剔除前序 {start_idx} 个块以节省时间与 Token！")
             
        return global_toc_md, start_idx

    def _run_step1_outline_extraction(self, pages: List[Dict], start_idx: int, global_toc_md: str) -> List:
        """执行步骤一：调用大模型通过滑窗扫描全文，提取统一的大纲树 AST 节点列"""
        self.logger.info("[*] [步骤 1] 正在发起全局 Markdown 抽取的 Map 线程池...")
        
        chunk_size = 5
        step_size = 4  # 滑动窗口每次前进一步保留一页重合（1-5，5-9）
        outline_md_parts = []
        previous_last_node_context = None
        
        pbar_s1 = tqdm(range(start_idx, len(pages), step_size), desc="分块大纲提取管道", unit="区块", dynamic_ncols=True)
        for i in pbar_s1:
            chunk_pages = pages[i : i + chunk_size]
            if not chunk_pages:
                break
                
            actual_end = min(i + chunk_size, len(pages))
            chunk_text = "\n".join([f"【第{p['page_num']}页】\n{p['text']}" for p in chunk_pages])
            pbar_s1.set_description(f"范围页码: {i+1}-{actual_end}")
            
            if global_toc_md:
                chunk_sys_prompt = STEP1_WITH_TOC_SYSTEM_PROMPT
                chunk_prompt = f"【全局标准实体目录】:\n{global_toc_md}\n\n【当前需要解析的局部片段（第 {i+1} 至 {actual_end} 页）】:\n{chunk_text}"
            else:
                chunk_sys_prompt = STEP1_SYSTEM_PROMPT
                chunk_prompt = f"以下是一份大卷白皮书的局部第 {i+1} 至 {actual_end} 页碎片片段。请严格抽取这当中的 Markdown 大纲结构(含页码及深度摘要)。\n"
                if previous_last_node_context:
                    chunk_prompt += f"\n【重要前置上下文】\n上一段落最后所处的对应大纲节点为 `{previous_last_node_context['path']}`，其内容摘要为 `{previous_last_node_context['summary']}`。\n"
                    chunk_prompt += "如果本段页码的内容是该层级的延续且没有出现更高层级/同层级的新标题，请默认将其归属于该层级下继续延伸，并保持你生成的 `#` 标题层级深度的相对正确。\n"
                    chunk_prompt += "【内容重合提示】为了保持上下文连贯，本分块可能包含前一段落末尾（一页左右）的重叠内容。如果遇到与前序摘要描述相同的重复段落，请结合上下文顺延，不要将其当作全新的章节重启。\n"
                chunk_prompt += f"\n【文本内容如下】:\n{chunk_text}"
            
            # --- 重载失败恢复重试与存盘机制 (Step 1) ---
            chunk_success = False
            for attempt in range(MAX_RETRIES):
                chunk_md = call_llm(chunk_sys_prompt, chunk_prompt, temperature=0.2)
                if chunk_md:
                    try:
                        parsed_nodes = parse_markdown_outline(chunk_md)
                        leafs = get_leaf_nodes(parsed_nodes)
                        if leafs:
                            last_leaf = leafs[-1]
                            previous_last_node_context = {
                                "path": last_leaf.path,
                                "summary": last_leaf.summary
                            }
                    except Exception as e:
                        self.logger.warning(f"    [!] 中间层级树解析获取 last_node 失败: {e}")

                    inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step1_batch_{i+1}_to_{actual_end}.md")
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
        
        with open(os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step1_Full_Rebuilt_Outline.md"), "w", encoding="utf-8") as f:
            f.write(outline_md)
            
        if not outline_md.strip():
            self.logger.error("[x] 整个第一步循环最终未能沉淀任何目录树格式数据，发生核心断桥级故障！")
            return []
            
        self.logger.info("[v] 第一步滑动窗口切片大纲完成！全局 AST 结构树解析即将加载...")
        root_nodes = parse_markdown_outline(outline_md)
        return get_leaf_nodes(root_nodes)

    def _run_step2_and_3_augmentation_and_review(self, leaf_nodes: List, pages: List[Dict]) -> List[Dict]:
        """执行步骤二与三：针对底层节点精准剥离增强，防污染审计排版以及超级汇总表的合并录入"""
        self.logger.info(f"[*] 解析结束。统计具备增强价值的末端独立模块 {len(leaf_nodes)} 个，开始分发到强化审核工厂。")
        final_data = []
        pbar = tqdm(leaf_nodes, desc="知识审核增强进度", unit="底层原子节点", dynamic_ncols=True)
        
        aggregated_md_path = os.path.join(self.output_dir, "Dify_知识库增强底座_汇总文件.md")
        with open(aggregated_md_path, "w", encoding="utf-8") as f:
            f.write("# 针对知识库增强的解构底座汇总文件\n\n")

        for i, node in enumerate(pbar):
            node_name_short = node.title[:15] + ".." if len(node.title) > 15 else node.title
            pbar.set_description(f"处理挂载: {node_name_short}")
            
            self.logger.info(f"\n-----------------------------------------------------------")
            self.logger.info(f"[*] 提取第 {i+1}/{len(leaf_nodes)} 支点上下文 | 逻辑路径: [{node.arrow_path}]")
            
            target_idx = -1
            for idx, p in enumerate(pages):
                if str(p['page_num']) == str(node.page_num).strip():
                    target_idx = idx
                    break
                    
            if target_idx != -1:
                start_p = max(0, target_idx - 1)
                end_p = min(len(pages), target_idx + 2)
                original_content = "\n".join([f"【第{p['page_num']}页】\n{p['text']}" for p in pages[start_p:end_p]])
            else:
                original_content = ""

            if not original_content.strip():
                self.logger.warning(f"    [!] 物理文本页码越界映射！系统将用自身的单段底层摘要回退覆盖其事实缺失。")
                original_content = node.summary

            self.logger.info(f"    -> [步骤 2] 请求主大语言引擎进行精准切割分离，提炼纯粹的局部知识...")
            custom_cmd = getattr(config, "CUSTOM_CHAPTER_PROMPT", "提取该章节的核心操作流程、业务逻辑与关键技术细节。确保描述客观、准确，逻辑连贯。")
            step2_user_prompt = f"【目标处理章节名称】：{node.title}\n【自定义抽取指令】：{custom_cmd}\n\n【物理原文片段 (可能包含前后页他章语境)】:\n{original_content}"
            
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

            self.logger.info(f"    -> [步骤 3] 数据流动到第二台 LLM 引擎负责 Markdown 标准层级化质检组装...")
            step3_user_prompt = f"【章节信息卡】\n名称: {node.title}\n路径: {node.arrow_path}\n页码: {node.page_num}\n摘要: {node.summary}\n\n【第一轮初步提取的内容报告】:\n{content_report}"
            
            reviewed_res = None
            for attempt in range(MAX_RETRIES):
                reviewed_res = call_llm(STEP3_SYSTEM_PROMPT, step3_user_prompt, temperature=0.1) 
                if reviewed_res:
                    inter_file = os.path.join(INTERMEDIATE_DIR, f"{self.run_prefix}step3_node_{i+1}_final_reviewed.md")
                    with open(inter_file, "w", encoding="utf-8") as f:
                        f.write(reviewed_res)
                    break
                else:
                    self.logger.warning(f"    [!] 审查模型引擎连接崩溃或空出，请求重连 {attempt+1}/{MAX_RETRIES}...")
            
            if not reviewed_res:
                self.logger.error("    [x] 针对节点规范化检验失败！系统丢弃此点！")
                continue
                
            with open(aggregated_md_path, "a", encoding="utf-8") as f:
                if "```markdown" in reviewed_res:
                    reviewed_res = reviewed_res.replace("```markdown", "").replace("```", "")
                f.write(reviewed_res.strip() + "\n\n")
                
            import re
            record_info = {}
            for lvl_idx, title in enumerate(node.arrow_path.split(' ->> ')):
                record_info[f"{lvl_idx+1}级标题"] = title.strip()
                
            keyword_match = re.search(r'^##\s+关键词\s*\n(.*?)(?=\n##|\Z)', reviewed_res, re.DOTALL | re.MULTILINE)
            record_info["核心关键词"] = keyword_match.group(1).strip() if keyword_match else "未提取"
            
            record_info["对应页码"] = node.page_num
            
            summary_match = re.search(r'^##\s+深度摘要\s*\n(.*?)(?=\n##|\Z)', reviewed_res, re.DOTALL | re.MULTILINE)
            record_info["底层节点摘要"] = summary_match.group(1).strip() if summary_match else node.summary
            
            content_match = re.search(r'^##\s+章节内容\s*\n(.*)', reviewed_res, re.DOTALL | re.MULTILINE)
            record_info["完整增强内容"] = content_match.group(1).strip() if content_match else ""
            
            final_data.append(record_info)
            self.logger.info(f"    [v] 属于 {node.title} 的单体重构流装组完毕。")

        return final_data

    def run(self):
        """执行完整的高危知识库提炼重构流水线引擎"""
        self.logger.info(f"========== 解析任务启动 ==========")
        self.logger.info(f"[*] 侦测目标文档: {self.file_path}")
        self.logger.info(f"[*] 缓存重试次数设定: {MAX_RETRIES} 次")
        self.logger.info(f"[*] 中间文件持久化保存路径: {INTERMEDIATE_DIR}")
        
        try:
            pages = extract_text_with_pages(self.file_path)
            self.logger.info(f"[v] 文档解析引擎工作正常，共识别提取了 {len(pages)} 个页块容纳。")
        except Exception as e:
            self.logger.error(f"[x] 文档解析直接发生错误: {e}", exc_info=True)
            return

        # Phase 0: 实体书目抽取探针
        global_toc_md, start_idx = self._run_step0_toc_extraction(pages)
        
        # Phase 1: Markdown AST树滑动构建
        leaf_nodes = self._run_step1_outline_extraction(pages, start_idx, global_toc_md)
        if not leaf_nodes:
            self.logger.warning("[!] 树干被系统发现但丢失了任何具有细分知识意义的分支/底层，生成树彻底被折断。")
            return
            
        # Phase 2 & 3: 绝对精准的内容切分洗底与 Markdown 审查汇总
        final_data = self._run_step2_and_3_augmentation_and_review(leaf_nodes, pages)

        # Phase 4: 使用原生工程将高度规范的 Pandas CSV/Excel 底仓生成输出
        self.logger.info(f"\n[*] [步骤 4] 所有切片和质检完成，触发一键全量 Pandas 报表生成...")
        self._export_to_excel(final_data)
        self.logger.info("========== Dify-PreProcessor 流畅跨越全部高危障碍线，顺利着陆！ ==========")


    def _export_to_excel(self, final_data: List[Dict]):
        if not final_data:
            self.logger.warning("[!] 当前批流式没有收集到任何合法载入字典的表报数据，终止渲染操作。")
            return
            
        self.logger.info("    -> [步骤 4] 清洗完结：驱动纯代码底座 Pandas 生成矩阵映射结构，100%防止幻觉转换故障...")
        
        try:
            df = pd.DataFrame(final_data)
            
            title_cols = [c for c in df.columns if '级标题' in c]
            title_cols = sorted(title_cols, key=lambda x: int(x.split('级')[0]))
            
            other_cols = ["底层节点摘要", "对应页码", "核心关键词", "完整增强内容"]
            final_cols = [c for c in title_cols + other_cols if c in df.columns]
            
            df = df.reindex(columns=final_cols)
            
            excel_path = os.path.join(self.output_dir, "Dify_知识库增强底座.xlsx")
            df.to_excel(excel_path, index=False)
            self.logger.info(f"    [v] 构建物理结构最终映射成功，Excel 数据舱极速落盘位至: {excel_path}")
        except Exception as e:
            self.logger.error(f"    [x] 落盘 Excel 数据舱时产生严重的结构兼容错误: {e}", exc_info=True)
