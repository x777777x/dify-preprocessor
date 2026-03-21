import os
import sys, httpx
from openai import OpenAI

# 将项目根目录加入到 sys.path 以便导入配置文件 config.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

def call_llm(system_prompt: str, user_prompt: str, model: str = None, temperature: float = 0.3) -> str:
    """
    统一的LLM调用封装接口，使用通用的 OpenAI API 格式。
    默认采用低 temperature 以保证生成的客观、严谨以及指令遵循度增强。
    """
    # 从单独的配置对象中抽离独立变量
    api_key = config.OPENAI_API_KEY
    base_url = config.OPENAI_API_BASE

    if not model:
        model = getattr(config, "LLM_MODEL", "gpt-4o")
    
    http_client = httpx.Client(
        proxies={},  # 空代理配置
        transport=httpx.HTTPTransport(local_address="0.0.0.0") # 确保绑定本地
    )

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=120.0, http_client=http_client)
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=temperature,
            stream=True  # 启用流式生成
        )
        
        full_content = ""
        # 为了更好地在终端展示打字机效果，不经过 logging 格式化直接输出字元
        print("\n    [LLM 流式打字机] ", end="", flush=True)
        
        for chunk in response:
            if chunk.choices and len(chunk.choices) > 0:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    text_chunk = delta.content
                    full_content += text_chunk
                    # 实时输出到控制台并立即刷新缓冲
                    print(text_chunk, end="", flush=True)
                    
        print("\n    [本轮生成结束]\n", flush=True)
        
        return full_content
    except Exception as e:
        print(f"[!] LLM API 调用失败报错: {e}")
        return ""
