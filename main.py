import os
from src.processor import DifyPreProcessor

if __name__ == "__main__":
    print("="*60)
    print(" Dify-PreProcessor : 智能化应用知识库前置增强数据装载平台")
    print("="*60)
    
    # 静态业务入口，由于 API 配置已经抽离出 config.py，主代码在此只需要关心逻辑管道
    # 指向你要测试的具体本地文件路径
    # docs_path = r"C:\Users\60544\Downloads\达梦时序数据库DMTDM技术白皮书（中文版）v3-250905.pdf"
    docs_path = r"E:\家庭文件\侵权纠纷\关于被告庭审抗辩意见及司法鉴定申请的书面质证与异议意见.docx"

    output_directory = "./dify_processed_output"
    
    if not os.path.exists(docs_path):
        print(f"[!] 警告: 测试文件 '{docs_path}' 不存在。请替换此处文件位置或放入 PDF。")
    else:
        app = DifyPreProcessor(file_path=docs_path, output_dir=output_directory)
        app.run()
