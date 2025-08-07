#!/bin/bash

# Flink 流处理示例批量测试脚本

set -e

echo "=========================================="
echo "    Flink 流处理示例批量测试"
echo "=========================================="

# 配置参数
FLINK_HOME=${FLINK_HOME:-"/usr/local/service/flink"}
EXAMPLES_DIR="$FLINK_HOME/examples/streaming"
TEST_RESULTS_DIR="/tmp/flink-streaming-test-results"
CURRENT_DIR=$(dirname "$(readlink -f "$0")")

# 创建结果目录
mkdir -p $TEST_RESULTS_DIR

echo "测试环境信息:"
echo "- Flink Home: $FLINK_HOME"
echo "- Examples目录: $EXAMPLES_DIR"
echo "- 结果目录: $TEST_RESULTS_DIR"
echo "- 当前目录: $CURRENT_DIR"

# 检查Flink环境
if [ ! -d "$FLINK_HOME" ]; then
    echo "错误: FLINK_HOME目录不存在: $FLINK_HOME"
    exit 1
fi

if [ ! -d "$EXAMPLES_DIR" ]; then
    echo "错误: Streaming examples目录不存在: $EXAMPLES_DIR"
    exit 1
fi

echo "✓ 环境检查通过"

# 测试用例定义
declare -A EXAMPLES=(
    ["WordCount.jar"]="基础流处理 - 单词计数"
    ["SocketWindowWordCount.jar"]="Socket流窗口计数"
    ["SessionWindowing.jar"]="会话窗口处理"
    ["StateMachineExample.jar"]="状态机示例"
    ["TopSpeedWindowing.jar"]="最高速度窗口"
    ["WindowJoin.jar"]="窗口连接"
    ["Iteration.jar"]="迭代处理"
)

# 测试统计
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0
declare -a FAILED_LIST=()
declare -a SKIPPED_LIST=()

# 记录开始时间
START_TIME=$(date +%s)

echo ""
echo "开始执行测试..."

# 执行测试函数
run_test() {
    local jar_name="$1"
    local description="$2"
    local test_script="$3"
    
    echo ""
    echo "===========================================" 
    echo "测试 $jar_name"
    echo "描述: $description"
    echo "==========================================="
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local test_start_time=$(date +%s)
    
    # 检查JAR文件是否存在
    if [ ! -f "$EXAMPLES_DIR/$jar_name" ]; then
        echo "❌ JAR文件不存在: $jar_name"
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
        SKIPPED_LIST+=("$jar_name (文件不存在)")
        return 1
    fi
    
    echo "✓ 找到JAR文件: $jar_name"
    
    # 检查测试脚本是否存在
    if [ ! -f "$test_script" ]; then
        echo "⚠️  测试脚本不存在: $test_script"
        echo "跳过自动化测试，仅验证JAR文件可用性"
        
        # 简单验证JAR文件
        if java -jar "$EXAMPLES_DIR/$jar_name" --help >/dev/null 2>&1 || \
           $FLINK_HOME/bin/flink info "$EXAMPLES_DIR/$jar_name" >/dev/null 2>&1; then
            echo "✅ JAR文件验证通过"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo "❌ JAR文件验证失败"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            FAILED_LIST+=("$jar_name (JAR验证失败)")
        fi
        return 0
    fi
    
    echo "✓ 找到测试脚本: $test_script"
    
    # 执行测试脚本
    local log_file="$TEST_RESULTS_DIR/${jar_name%.*}.log"
    echo "执行测试脚本，日志保存到: $log_file"
    
    if timeout 300s bash "$test_script" > "$log_file" 2>&1; then
        local test_end_time=$(date +%s)
        local test_duration=$((test_end_time - test_start_time))
        echo "✅ 测试通过 (耗时: ${test_duration}秒)"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        local test_end_time=$(date +%s)
        local test_duration=$((test_end_time - test_start_time))
        echo "❌ 测试失败 (耗时: ${test_duration}秒)"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        FAILED_LIST+=("$jar_name")
        
        # 显示错误信息
        echo ""
        echo "错误信息 (最后10行):"
        tail -10 "$log_file"
    fi
}

# 执行所有测试
for jar_name in "${!EXAMPLES[@]}"; do
    description="${EXAMPLES[$jar_name]}"
    
    # 确定测试脚本路径
    case "$jar_name" in
        "WordCount.jar")
            test_script="$CURRENT_DIR/test-wordcount.sh"
            ;;
        "SocketWindowWordCount.jar")
            test_script="$CURRENT_DIR/test-socket-wordcount.sh"
            ;;
        "SessionWindowing.jar")
            test_script="$CURRENT_DIR/test-session-windowing.sh"
            ;;
        "StateMachineExample.jar")
            test_script="$CURRENT_DIR/test-state-machine.sh"
            ;;
        "TopSpeedWindowing.jar")
            test_script="$CURRENT_DIR/test-top-speed.sh"
            ;;
        "WindowJoin.jar")
            test_script="$CURRENT_DIR/test-window-join.sh"
            ;;
        "Iteration.jar")
            test_script="$CURRENT_DIR/test-iteration.sh"
            ;;
        *)
            test_script=""
            ;;
    esac
    
    run_test "$jar_name" "$description" "$test_script"
done

# 计算总耗时
END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

# 生成测试报告
echo ""
echo "=========================================="
echo "           测试执行完成"
echo "=========================================="

# 基本统计
echo "测试统计:"
echo "- 总测试数: $TOTAL_TESTS"
echo "- 通过: $PASSED_TESTS"
echo "- 失败: $FAILED_TESTS"
echo "- 跳过: $SKIPPED_TESTS"
echo "- 总耗时: ${TOTAL_DURATION}秒"

# 计算成功率
if [ $TOTAL_TESTS -gt 0 ]; then
    SUCCESS_RATE=$(( (PASSED_TESTS * 100) / TOTAL_TESTS ))
    echo "- 成功率: ${SUCCESS_RATE}%"
else
    SUCCESS_RATE=0
    echo "- 成功率: N/A"
fi

# 失败的测试
if [ $FAILED_TESTS -gt 0 ]; then
    echo ""
    echo "失败的测试:"
    for failed_test in "${FAILED_LIST[@]}"; do
        echo "  ❌ $failed_test"
    done
fi

# 跳过的测试
if [ $SKIPPED_TESTS -gt 0 ]; then
    echo ""
    echo "跳过的测试:"
    for skipped_test in "${SKIPPED_LIST[@]}"; do
        echo "  ⚠️  $skipped_test"
    done
fi

# 测试结果文件
echo ""
echo "测试结果文件:"
echo "- 结果目录: $TEST_RESULTS_DIR"
echo "- 查看所有日志: ls -la $TEST_RESULTS_DIR"

if [ $FAILED_TESTS -gt 0 ]; then
    echo ""
    echo "查看失败测试的详细日志:"
    for failed_test in "${FAILED_LIST[@]}"; do
        jar_name=$(echo "$failed_test" | cut -d' ' -f1)
        log_file="$TEST_RESULTS_DIR/${jar_name%.*}.log"
        if [ -f "$log_file" ]; then
            echo "  cat $log_file"
        fi
    done
fi

# 生成HTML报告
generate_html_report() {
    local html_file="$TEST_RESULTS_DIR/streaming-test-report.html"
    
    cat > "$html_file" << EOF
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Flink流处理示例测试报告</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1000px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }
        .header { text-align: center; color: #333; border-bottom: 2px solid #ddd; padding-bottom: 20px; }
        .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
        .metric { background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }
        .metric h3 { margin: 0; color: #666; }
        .metric .value { font-size: 2em; font-weight: bold; margin: 10px 0; }
        .success { color: #28a745; }
        .danger { color: #dc3545; }
        .warning { color: #ffc107; }
        .info { color: #17a2b8; }
        .test-list { margin: 20px 0; }
        .test-item { margin: 10px 0; padding: 10px; border-left: 4px solid #ddd; background: #f8f9fa; }
        .test-pass { border-left-color: #28a745; }
        .test-fail { border-left-color: #dc3545; }
        .test-skip { border-left-color: #ffc107; }
        .footer { text-align: center; margin-top: 30px; color: #666; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Flink流处理示例测试报告</h1>
            <p>测试时间: $(date)</p>
        </div>
        
        <div class="summary">
            <div class="metric">
                <h3>总测试数</h3>
                <div class="value info">$TOTAL_TESTS</div>
            </div>
            <div class="metric">
                <h3>通过</h3>
                <div class="value success">$PASSED_TESTS</div>
            </div>
            <div class="metric">
                <h3>失败</h3>
                <div class="value danger">$FAILED_TESTS</div>
            </div>
            <div class="metric">
                <h3>跳过</h3>
                <div class="value warning">$SKIPPED_TESTS</div>
            </div>
            <div class="metric">
                <h3>成功率</h3>
                <div class="value info">${SUCCESS_RATE}%</div>
            </div>
            <div class="metric">
                <h3>总耗时</h3>
                <div class="value info">${TOTAL_DURATION}s</div>
            </div>
        </div>
        
        <div class="test-list">
            <h2>测试详情</h2>
EOF

    # 添加测试结果详情
    for jar_name in "${!EXAMPLES[@]}"; do
        description="${EXAMPLES[$jar_name]}"
        
        # 判断测试状态
        if [[ " ${FAILED_LIST[@]} " =~ " ${jar_name} " ]]; then
            status="test-fail"
            status_text="❌ 失败"
        elif [[ " ${SKIPPED_LIST[@]} " =~ " ${jar_name} " ]]; then
            status="test-skip"
            status_text="⚠️ 跳过"
        else
            status="test-pass"
            status_text="✅ 通过"
        fi
        
        cat >> "$html_file" << EOF
            <div class="test-item $status">
                <h3>$jar_name - $description</h3>
                <p>状态: $status_text</p>
            </div>
EOF
    done
    
    cat >> "$html_file" << EOF
        </div>
        
        <div class="footer">
            <p>报告生成时间: $(date)</p>
            <p>Flink Home: $FLINK_HOME</p>
        </div>
    </div>
</body>
</html>
EOF

    echo "✓ HTML报告已生成: $html_file"
}

# 生成HTML报告
echo ""
echo "生成HTML测试报告..."
generate_html_report

# 建议和下一步
echo ""
echo "建议的下一步操作:"

if [ $FAILED_TESTS -gt 0 ]; then
    echo "1. 检查失败测试的详细日志"
    echo "2. 验证Flink集群状态和配置"
    echo "3. 检查依赖服务 (如Socket端口)"
fi

if [ $SKIPPED_TESTS -gt 0 ]; then
    echo "4. 创建缺失的测试脚本"
    echo "5. 完善测试用例覆盖"
fi

echo "6. 查看HTML报告: $TEST_RESULTS_DIR/streaming-test-report.html"
echo "7. 单独运行特定测试进行调试"

# 退出状态
if [ $FAILED_TESTS -eq 0 ]; then
    echo ""
    echo "🎉 所有测试执行完成！"
    exit 0
else
    echo ""
    echo "⚠️  有 $FAILED_TESTS 个测试失败"
    exit 1
fi