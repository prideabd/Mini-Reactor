import time
import requests
import json
import os
import re
from openai import OpenAI

# =====================================================================
# 🧠 Mini-Reactor AI 影子守护进程 (DeepSeek-V4-Flash 生产线完全体)
# =====================================================================

# 1. 初始化大模型客户端
api_key_from_env = os.getenv("DEEPSEEK_API_KEY")
client = OpenAI(
    api_key=api_key_from_env,
    base_url="https://api.deepseek.com/v1",
)

# 2. C++ 引擎监听的物理本地回环地址
CPP_SERVER_CUL = "http://127.0.0.1:8080"
def run_agent_guard_loop():
    print("========================================================")
    print("🟢 [AI Agent]: 智能运维与舆情控制中心守护进程已成功点火！")
    print("📡 [AI Agent]: 正在死循环常驻后台，密切守望 C++ Reactor 核心...")
    print("========================================================")

    # 记录上次处理完的最大留言序列号，初始值为 -1
    last_processed_seq = -1
    # 记录上次拉取时的历史累计总请求数，用于物理对账计算瞬时高并发
    last_total_requests = 0

    while True:
        try:
            # ────────────────────────────────────────────────────────
            # 🚀 动作一：主动定时嗅探，拉取 C++ 纯内存全测序数据
            # ────────────────────────────────────────────────────────
            telemetry_url = f"{CPP_SERVER_CUL}/api/agent/telemetry"
            try:
                telemetry_response = requests.get(telemetry_url, timeout=1.5)
            except requests.exceptions.ConnectionError:
                print("⚠️ [AI Agent]: 撞击 C++ 网关失败，可能 C++ 核心尚未点火或正在重启，3秒后重试...")
                time.sleep(3)
                continue
            if telemetry_response.status_code != 200:
                print(f"⚠️ [AI Agent]: C++ 网关拒绝连接，HTTP 状态码: {telemetry_response.status_code}")
                time.sleep(3)
                continue

            # 解析 C++ 返回的页面信息
            system_data = telemetry_response.json()
            metrics = system_data.get("metrics", {})
            comments = system_data.get("comments", {})

            # 通过前后两次快照差计算真实并发
            current_total_requests = metrics.get("total_requests", 0)
            instant_pulse_requests = current_total_requests - last_total_requests
            if instant_pulse_requests < 0:
                instant_pulse_requests = 0
            last_total_requests = current_total_requests

            # 设定一个并发洪峰阈值：3秒周期内如果累计涌入超过 150 个请求，即断定遭遇高并发
            is_high_concurrency = instant_pulse_requests > 500
            
            # 新旧评论号对比
            current_sequence = metrics.get('current_sequence', 0)
            has_new_comments = current_sequence > last_processed_seq

            # ────────────────────────────────────────────────────────
            # 🧠 动作二：构建结构化 Prompt，进行严苛的智慧风控审计
            # ────────────────────────────────────────────────────────
            # 唯有当【有新评论进来】或者【大盘遭遇高并发洪峰冲击】时，才放行给大模型
            if comments and (has_new_comments or is_high_concurrency):

                # 打印当前唤醒大模型的真实运维上下文
                if has_new_comments:
                    print(f"📡 [AI 审计]: 检测到增量新评论入场 (序列号变化: {last_processed_seq} -> {current_sequence})")
                if is_high_concurrency:
                    print(f"🚨 [AI 预警]: 大盘遭遇高并发脉冲轰炸！(当前3秒周期内净增请求: {instant_pulse_requests} 次)")

                # 一旦放行进入大模型审计，立刻将当前的序列号同步锁死
                last_processed_seq = current_sequence

                # 构建 Prompt
                prompt = f"""
                [系统核心监控快照]
                - 总请求累计量: {current_total_requests}
                - 留言板当前最大序列号:{current_sequence}
                - 当前瞬时并发冲量(3s内): {instant_pulse_requests}
                
                [待审计的最新内存留言列表]
                {json.dumps(comments, ensure_ascii=False, indent=2)}
                
                [你的职责]
                你现在是该网关的 AI 守护。请像素级审查上述留言列表。
                判定标准：是否有包含脏话、谩骂、恶意人身攻击、违法政治敏感、或小广告乱码的违规内容？
                
                请严格按照以下 JSON 格式进行回答，不要吐出任何多余的废话和 markdown 标记（如 ```json）：
                {{
                  "has_violation": true/false,
                  "target_seqs": [10, 15],  // 👈 必须是数组！列出所有违规留言的 seq 数字，若无违规则为空数组 []
                  "reason": "你的中文诊断意见（限制在30字内）"
                }}
                """

                # 调用 deepseek-v4-flash 完成推理
                ai_response = client.chat.completions.create(
                    model="deepseek-v4-flash",
                    messages=[
                        {"role": "system", "content": "你是一个只输出标准 JSON 对象的自动化网关运维及网络安全风控 Agent。"},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"}, # 约束返回为 json 格式
                    stream=False,
                    timeout=15
                )

                # 解析大模型的推理结果
                raw_content = ai_response.choices[0].message.content

                try:
                    # 1. 🛡️ 终极过滤器：利用正则把所有不合规的裸露控制字符（ASCII 0-31，除了换行和制表符）物理剔除
                    clean_content = re.sub(r'[\x00-\x1F\x7F]', '', raw_content)
                    
                    # 2. 如果包含了 Markdown 标记，进行强行刮骨
                    clean_content = clean_content.replace("```json", "").replace("```", "").strip()
                    
                    # 3. 带上 strict=False 进行绝对解析
                    ai_decision = json.loads(clean_content, strict=False)
                except Exception as e:
                    # 4. 万一真的格式乱了，做一次兜底，防止进程坠毁或死循环暴击钱包
                    print(f"⚠️ [解析降级]: 大模型格式异常，执行人工放行。原因: {e}")
                    ai_decision = {"has_violation": False, "target_seq": -1, "reason": "解析失败兜底"}

                print(f"🤖 [AI 动态诊断波形]: {ai_decision.get('reason', '一切正常')}")

                # ────────────────────────────────────────────────────────
                # 🛡️ 动作三：如果大模型亮起红灯，Agent 立刻发起反向原子擦除
                # ────────────────────────────────────────────────────────
                if ai_decision.get("has_violation"):
                    # 优先获取数组，如果大模型犯傻给了老的单数格式，做一下降级兼容
                    target_seqs = ai_decision.get("target_seqs", [])

                    # 降级兼容：如果大模型死活只吐 target_seq 字段
                    if not target_seqs and "target_seq" in ai_decision:
                        val = ai_decision.get("target_seq")
                        # 如果吐出来的已经是列表，直接用；如果是单个数字，套成列表
                        if isinstance(val, list):
                            target_seqs = val
                        elif val != -1:
                            target_seqs = [val]

                    # 删除 -1 无效数据
                    valid_seqs = [seq for seq in target_seqs if isinstance(seq, int) and seq != -1]

                    if valid_seqs:
                        print(f"🚨 [AI 警报]: 准备对序列号 {valid_seqs} 发起饱和式批量拦截！")

                        control_url = f"{CPP_SERVER_CUL}/api/agent/control"
                        control_payload = {"block_seqs": valid_seqs}

                        try:
                            control_response = requests.post(control_url, json=control_payload, timeout=2)
                            print(f"🎛️ [C++ 批量擦除反馈]: {control_response.text.strip()}")
                        except Exception as comm_err:
                            print(f"❌ [反向控制通信坠毁]: {comm_err}")
            else:
                # 内存池干净空无一物，AI 默默守护看盘
                print(f"📊 [AI 动态诊断波形]: 当前内存留言池纯净。网关请求累计量: {metrics.get('total_requests', 0)}")

        except Exception as e:
            print(f"❌ [AI Agent 守护异常]: {str(e)}")

        # 🛡️ 物理保险锁：强制原地死等 3 秒
        # 既给高并发 C++ Reactor 引擎留出收集吞吐指标的呼吸周期，又保障你的钱包绝对不会因为死循环而偷跑额度！
        time.sleep(3)

if __name__== "__main__":
    run_agent_guard_loop()