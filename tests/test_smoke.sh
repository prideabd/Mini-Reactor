#!/usr/bin/env bash
# =============================================================
#  Mini-Reactor 部署后测试脚本  (功能 P0~P5 + 压测 P6)
# -------------------------------------------------------------
#  用法:
#     chmod +x test_smoke.sh
#     BASE=http://127.0.0.1:8080 ./test_smoke.sh
#
#  常用环境变量:
#     BASE            服务地址            (默认 http://127.0.0.1:8080)
#     ADMIN_TOKEN     管理鉴权头           (默认 Bearer MY_SUPER_SECRET_TOKEN_2026)
#     BLACKLIST_FILE  blacklist.txt 路径   (给了才跑「文件级黑名单 + 热加载」用例)
#     DB_FILE         comments.db 路径     (给了且有 sqlite3 才跑「IP 落盘」校验)
#     SERVER_PID      服务进程 PID         (给了才跑 SIGHUP; 否则自动 pgrep web_server)
#     TEST_IP         服务端看到的本机 IP  (默认 127.0.0.1, 用于 IP 维度用例)
#
#  选项:
#     --safe     只读模式: 跳过一切会改数据/黑名单/熔断的用例
#     --stress   额外跑 P6 压测 (需 wrk 或 ab)
#     -h|--help  帮助
# =============================================================
set -uo pipefail

BASE="${BASE:-http://127.0.0.1:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-Bearer MY_SUPER_SECRET_TOKEN_2026}"
TEST_IP="${TEST_IP:-127.0.0.1}"
BLACKLIST_FILE="${BLACKLIST_FILE:-}"
DB_FILE="${DB_FILE:-}"
SERVER_PID="${SERVER_PID:-}"
SAFE=0; STRESS=0

for a in "$@"; do
  case "$a" in
    --safe)   SAFE=1 ;;
    --stress) STRESS=1 ;;
    -h|--help) grep '^#' "$0" | sed 's/^#//'; exit 0 ;;
    *) echo "未知参数: $a (用 --help 查看用法)"; exit 2 ;;
  esac
done

# ---------- 颜色 & 计数 ----------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; B=$'\033[36m'; D=$'\033[2m'; N=$'\033[0m'
else G=; R=; Y=; B=; D=; N=; fi
PASS=0; FAIL=0; SKIP=0
FAILED_NAMES=()

pass(){ PASS=$((PASS+1)); echo "  ${G}✔ PASS${N} $1"; }
fail(){ FAIL=$((FAIL+1)); FAILED_NAMES+=("$1"); echo "  ${R}✘ FAIL${N} $1"; }
skip(){ SKIP=$((SKIP+1)); echo "  ${Y}⊘ SKIP${N} $1"; }
phase(){ echo; echo "${B}══ $1 ══${N}"; }

# ---------- HTTP helper: 设置 HTTP_CODE / HTTP_BODY ----------
HTTP_CODE=""; HTTP_BODY=""
http(){
  # 用法: http METHOD URL [额外 curl 参数...]
  local method="$1" url="$2"; shift 2
  local raw
  raw=$(curl -s -m 10 -w $'\n%{http_code}' -X "$method" "$@" "$url" 2>/dev/null)
  HTTP_CODE="${raw##*$'\n'}"
  HTTP_BODY="${raw%$'\n'*}"
}
expect_code(){ # expect_code 期望码 描述
  if [[ "$HTTP_CODE" == "$1" ]]; then pass "$2 ${D}(HTTP $HTTP_CODE)${N}"
  else fail "$2 — 期望 HTTP $1, 实得 ${HTTP_CODE:-无响应}"; fi
}
expect_body(){ # expect_body 子串 描述
  if grep -qiF -- "$1" <<<"$HTTP_BODY"; then pass "$2"
  else fail "$2 — 响应未包含 '$1' ${D}=> ${HTTP_BODY:0:120}${N}"; fi
}

reload_http(){ http POST "$BASE/api/admin/reload_blacklist" -H "Authorization: $ADMIN_TOKEN"; }
post_comment(){ # post_comment 昵称 内容
  http POST "$BASE/api/comment" -H 'Content-Type: application/x-www-form-urlencoded' \
       --data-urlencode "name=$1" --data-urlencode "content=$2"
}
get_seq_by_nick(){ # 在 telemetry 里找某昵称对应的最新 seq
  http GET "$BASE/api/agent/telemetry"
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg n "$1" '.comments[]?|select(.nick==$n)|.seq' <<<"$HTTP_BODY" | tail -1
  elif command -v python3 >/dev/null 2>&1; then
    python3 -c "import sys,json;d=json.load(sys.stdin);xs=[c['seq'] for c in d.get('comments',[]) if c.get('nick')==sys.argv[1]];print(xs[-1] if xs else '')" "$1" 2>/dev/null <<<"$HTTP_BODY"
  fi
}

echo "${B}Mini-Reactor 测试${N}  目标=${BASE}  $([[ $SAFE == 1 ]] && echo '[只读模式]')"

# =============================================================
# P0 · 冒烟
# =============================================================
phase "P0 冒烟"
http GET "$BASE/api/monitor";                 expect_code 200 "GET /api/monitor 可达"; expect_body "uptime_seconds" "monitor 返回核心指标"
http GET "$BASE/";                            expect_code 200 "GET / 首页可达"
post_comment "smoke_tester" "hello mini-reactor"; expect_code 200 "POST /api/comment 成功"; expect_body "SUCCESS" "留言写入返回 SUCCESS"
http GET "$BASE/api/comment";                 expect_code 200 "GET /api/comment 列表可达"

# =============================================================
# P1 · 本地持久化 (重点: IP 落盘)
# =============================================================
phase "P1 本地持久化"
if [[ $SAFE == 1 ]]; then
  skip "P1 写库类用例 (只读模式)"
else
  MARK="persist_$(date +%s)"
  post_comment "$MARK" "ip-persist-check"; expect_code 200 "写入带标记的留言 ($MARK)"
  sleep 1   # 等异步落盘
  if [[ -n "$DB_FILE" ]] && command -v sqlite3 >/dev/null 2>&1; then
    ROW_IP=$(sqlite3 "$DB_FILE" "SELECT ip FROM comments WHERE nickname='$MARK' LIMIT 1;" 2>/dev/null)
    ROW_CNT=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM comments;" 2>/dev/null)
    if [[ -n "$ROW_IP" ]]; then pass "SQLite 中该留言 ip 列已落盘 ${D}(ip=$ROW_IP)${N}"
    else fail "SQLite 中 ip 列为空 — IP 持久化未生效 (检查 insert_sql 是否 4 列)"; fi
    [[ "${ROW_CNT:-0}" -gt 0 ]] && pass "comments 表有数据 ${D}($ROW_CNT 行)${N}" || fail "comments 表为空"
  else
    skip "SQLite 落盘校验 (未设 DB_FILE 或无 sqlite3)"
  fi
fi

# =============================================================
# P2 + P3 · 多维度黑名单 + 热加载
# =============================================================
phase "P2/P3 多维度黑名单 + 热加载"
# --- HTTP 热加载鉴权 (不依赖文件, 总是测) ---
reload_http; expect_code 200 "HTTP 热加载 (token 正确) -> 200"
http POST "$BASE/api/admin/reload_blacklist" -H "Authorization: Bearer WRONG"; expect_code 401 "HTTP 热加载 (错 token) -> 401"
http POST "$BASE/api/admin/reload_blacklist";                                  expect_code 401 "HTTP 热加载 (无 token) -> 401"

if [[ $SAFE == 1 ]]; then
  skip "文件级黑名单/SIGHUP 用例 (只读模式)"
elif [[ -z "$BLACKLIST_FILE" || ! -w "$BLACKLIST_FILE" ]]; then
  skip "文件级黑名单/SIGHUP 用例 (未设 BLACKLIST_FILE 或不可写)"
else
  BAK="${BLACKLIST_FILE}.testbak.$$"
  cp "$BLACKLIST_FILE" "$BAK"
  restore_bl(){ [[ -f "$BAK" ]] && cp "$BAK" "$BLACKLIST_FILE" && rm -f "$BAK" && reload_http >/dev/null 2>&1; }
  trap restore_bl EXIT

  # ---- 配置 A: 昵称 + 关键词 (不含 IP, 避免误伤本机其它用例) ----
  printf '%s\n' "# test config A" "[nick]" "__banned_nick__" "[ip]" "[kw]" "__spamkw__" > "$BLACKLIST_FILE"
  reload_http >/dev/null
  post_comment "__banned_nick__" "hi";              expect_code 403 "昵称维度拦截 -> 403"; expect_body "拦截" "昵称拦截返回安全提示"
  post_comment "normal_user" "this is __SPAMKW__";  expect_code 403 "关键词维度拦截(大小写不敏感) -> 403"
  post_comment "normal_user" "clean message ok";    expect_code 200 "干净留言放行 -> 200"; expect_body "SUCCESS" "干净留言写入成功"

  # ---- 配置 B: 仅 IP (验 IP 维度; 会拦掉本机全部投递, 测完即还原) ----
  printf '%s\n' "# test config B" "[nick]" "[ip]" "$TEST_IP" "[kw]" > "$BLACKLIST_FILE"
  reload_http >/dev/null
  post_comment "anyone" "should be blocked by ip"
  if [[ "$HTTP_CODE" == "403" ]]; then pass "IP 维度拦截 -> 403"
  else skip "IP 维度拦截 (服务端看到的客户端 IP 非 $TEST_IP? 可能走了反代, 设 TEST_IP 重试)"; fi

  # ---- SIGHUP 热加载 ----
  PID="$SERVER_PID"; [[ -z "$PID" ]] && PID=$(pgrep -f web_server 2>/dev/null | head -1)
  if [[ -n "$PID" ]]; then
    printf '%s\n' "# sighup test" "[nick]" "__sighup_nick__" "[ip]" "[kw]" > "$BLACKLIST_FILE"
    kill -HUP "$PID" 2>/dev/null && sleep 1
    post_comment "__sighup_nick__" "hi"
    if [[ "$HTTP_CODE" == "403" ]]; then pass "SIGHUP 热加载生效 (PID=$PID, 不重启) -> 403"
    else fail "SIGHUP 后新规则未生效 (PID=$PID) — 实得 HTTP $HTTP_CODE"; fi
  else
    skip "SIGHUP 用例 (未找到服务 PID, 可设 SERVER_PID)"
  fi

  restore_bl; trap - EXIT
  pass "blacklist.txt 已还原"
fi

# =============================================================
# P4 · Agent 反向控制闭环
# =============================================================
phase "P4 Agent 反向控制"
http GET "$BASE/api/agent/telemetry"; expect_code 200 "telemetry 可达"; expect_body '"ip"' "telemetry 含 ip 字段"; expect_body "metrics" "telemetry 含 metrics"
if [[ $SAFE == 1 ]]; then
  skip "熔断/擦除类用例 (只读模式)"
else
  # 熔断开
  http POST "$BASE/api/agent/control" -H 'Content-Type: application/json' -d '{"cooldown":true}'
  expect_code 200 "开启熔断"; expect_body "SYSTEM_COOLDOWN_ON" "熔断 action=ON"
  http GET "$BASE/api/monitor"; expect_body "CIRCUIT_BREAKER" "monitor 状态=CIRCUIT_BREAKER"
  post_comment "x" "during cooldown"; expect_body "熔断" "熔断期间投递被婉拒"
  # 熔断关
  http POST "$BASE/api/agent/control" -H 'Content-Type: application/json' -d '{"cooldown":false}'
  expect_body "SYSTEM_COOLDOWN_OFF" "熔断 action=OFF"
  http GET "$BASE/api/monitor"; expect_body "RUNNING" "monitor 恢复 RUNNING"

  # 批量擦除 + 自动拉黑
  ENICK="erase_$(date +%s)"
  post_comment "$ENICK" "to be erased"; sleep 1
  SEQ=$(get_seq_by_nick "$ENICK")
  if [[ -n "$SEQ" ]]; then
    http POST "$BASE/api/agent/control" -H 'Content-Type: application/json' -d "{\"block_seqs\":[$SEQ]}"
    expect_body "BATCH_ERASED" "批量擦除 seq=$SEQ 返回 BATCH_ERASED"
    http GET "$BASE/api/agent/telemetry"; expect_body "AI 已拦截" "被擦留言已标记为[AI 已拦截]"
  else
    skip "批量擦除 (无 jq/python3 解析 seq, 无法定位)"
  fi
fi

# =============================================================
# P5 · 安全 / 边界
# =============================================================
phase "P5 安全 / 边界"
http GET "$BASE/../../etc/passwd" --path-as-is; expect_code 403 "路径穿越拦截 -> 403"
http POST "$BASE/api/comment" -H 'Content-Type: text/plain' -d 'name=a&content=b'; expect_code 415 "非 urlencoded 表单 -> 415"
http GET "$BASE/__definitely_missing__.html"; expect_code 404 "不存在的静态资源 -> 404"
http POST "$BASE/api/agent/control" -H 'Content-Type: application/json' -d '{bad json'; expect_body "PARSE_ERROR" "非法 JSON 控制包 -> PARSE_ERROR (不崩)"

# =============================================================
# P6 · 压力测试 (可选)
# =============================================================
phase "P6 压力测试"
if [[ $STRESS == 1 ]]; then
  if command -v wrk >/dev/null 2>&1; then
    echo "${D}> wrk -t8 -c500 -d20s $BASE/api/stress${N}"
    wrk -t8 -c500 -d20s "$BASE/api/stress"
  elif command -v ab >/dev/null 2>&1; then
    echo "${D}> ab -n 50000 -c 500 $BASE/api/stress${N}"
    ab -n 50000 -c 500 "$BASE/api/stress"
  else
    skip "压测 (未安装 wrk 或 ab)"
  fi
else
  skip "压测未启用 (加 --stress 运行)"
  cat <<EOF
${D}  手动压测模板:
    吞吐:   wrk -t8 -c1000 -d30s $BASE/api/stress
    写路径: wrk -t8 -c500  -d30s -s post.lua $BASE/api/comment
    熔断:   持续加压至 QPS>10000 或 CPU>95%, 观察 /api/monitor 是否翻转 CIRCUIT_BREAKER,
            回落到 CPU<80% / QPS<7000 后是否自动恢复 RUNNING
    退出:   高压中 kill -INT <pid>, 确认后台写盘队列清空后再退出、无丢写
    竞争:   用 -fsanitize=thread 编一版跑一轮, 看有无 TSan 告警${N}
EOF
fi

# =============================================================
# 需人工确认的项 (脚本不自动重启服务)
# =============================================================
phase "需人工确认"
cat <<EOF
${Y}  □ 重启恢复: 重启进程后 GET /api/comment 与 telemetry, 确认最近 50 条回来、ip 仍在、序号从 max+1 续上
  □ 匿名极客护栏: 擦除一条「匿名极客」留言后, 确认 blacklist.txt 只新增了 IP、没有写入「匿名极客」昵称
  □ 批量持久化: 一次 block_seqs 擦除多条, 确认 blacklist.txt 只被重写一次、条目齐全且去重
  □ 反代场景: 若线上走 Nginx, getpeername 拿到的是代理 IP, IP 维度需配合 X-Forwarded-For${N}
EOF

# ---------- 汇总 ----------
phase "汇总"
TOTAL=$((PASS+FAIL))
echo "  通过 ${G}$PASS${N} / 失败 ${R}$FAIL${N} / 跳过 ${Y}$SKIP${N}   (有效断言 $TOTAL)"
if [[ $FAIL -gt 0 ]]; then
  echo "  ${R}失败项:${N}"; for n in "${FAILED_NAMES[@]}"; do echo "    - $n"; done
  exit 1
fi
echo "  ${G}全部通过 ✔${N}"
exit 0
