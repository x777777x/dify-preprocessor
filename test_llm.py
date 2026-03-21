import os
import sys

# 将项目根目录加入到 sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.llm_client import call_llm
import config

def test_connection():
    print(f"[*] 正在尝试连接本地大模型接口...")
    print(f"[-] 接口地址 (API_BASE): {config.OPENAI_API_BASE}")
    print(f"[-] 当前配置模型 (MODEL): {config.LLM_MODEL}")
    
    system_prompt = "你是一个专业的人工智能助手。"
    user_prompt = "你好！你能做些什么？请简短地用一两句话介绍你自己。"
    
    print("\n[*] 发送测试请求中 (这可能需要几秒钟冷启动模型)，请等待...")
    try:
        response = call_llm(system_prompt, user_prompt, temperature=0.2)
        
        print("\n================= 大模型返回结果 =================")
        if response:
            print(response)
        else:
            print("[x] 仅仅接收到了空响应或者请求失败。")
        print("==================================================")
    except Exception as e:
        print(f"\n[x] 测试请求彻底失败，捕获到致命异常：\n{e}")

if __name__ == "__main__":
    test_connection()
