#!/bin/bash

# Flink功能测试 - 执行所有测试用例

set -e

echo "=========================================="
echo "    Flink 功能测试 - 开始执行"
echo "=========================================="

# 记录测试开始时间
START_TIME=$(date +%s)

# 测试结果统计
TOTAL_TESTS=20
PASSED_TESTS=0
FAILED_TESTS=0
FAILED_TEST_LIST=()

# 设置环境
echo "设置测试环境..."
source ./setup-test-env.sh

if [ $? -ne 0 ]; then
    echo "❌ 环境设置失败，退出测试"
    exit 1
fi

# 编译Java测试用例
echo "编译Java测试用例..."
cd java-examples
mvn clean package -DskipTests
if [ $? -eq 0 ]; then
    echo "✅ Java测试用例编译成功"
    cd ..
else
    echo "❌ Java测试用例编译失败"
    cd ..
    exit 1
fi

# 创建测试结果目录
mkdir -p test-results
RESULT_FILE="test-results/test-results-$(date +%Y%m%d-%H%M%S).log"

echo "测试结果将保存到: $RESULT_FILE"
echo "开始时间: $(date)" > $RESULT_FILE

# 执行测试用例函数
run_test_case() {
    local test_num=$1
    local test_name=$2
    local test_script=$3
    
    echo ""
    echo "===========================================" | tee -a $RESULT_FILE
    echo "执行测试用例 $test_num: $test_name" | tee -a $RESULT_FILE
    echo "===========================================" | tee -a $RESULT_FILE
    
    local start_time=$(date +%s)
    
    if [ -f "$test_script" ]; then
        # 执行测试脚本
        chmod +x $test_script
        if $test_script >> $RESULT_FILE 2>&1; then
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            echo "✅ 测试用例 $test_num: 通过 (耗时: ${duration}s)" | tee -a $RESULT_FILE
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            echo "❌ 测试用例 $test_num: 失败 (耗时: ${duration}s)" | tee -a $RESULT_FILE
            FAILED_TESTS=$((FAILED_TESTS + 1))
            FAILED_TEST_LIST+=("$test_num: $test_name")
        fi
    else
        echo "⚠️  测试用例 $test_num: 脚本不存在 ($test_script)" | tee -a $RESULT_FILE
        FAILED_TESTS=$((FAILED_TESTS + 1))
        FAILED_TEST_LIST+=("$test_num: $test_name (脚本不存在)")
    fi
}

# 执行所有测试用例
echo "开始执行测试用例..."

# 测试用例1: WordCount
run_test_case 1 "基础流处理 - WordCount" "./run-test-case-1.sh"

# 测试用例2: 滑动窗口 (需要Java程序)
echo "执行测试用例2: 滑动窗口..." | tee -a $RESULT_FILE
if [ -f "java-examples/target/flink-test-examples-1.0.0.jar" ]; then
    $FLINK_HOME/bin/flink run -c com.example.SlidingWindowExample java-examples/target/flink-test-examples-1.0.0.jar >> $RESULT_FILE 2>&1
    if [ $? -eq 0 ]; then
        echo "✅ 测试用例2: 通过" | tee -a $RESULT_FILE
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo "❌ 测试用例2: 失败" | tee -a $RESULT_FILE
        FAILED_TESTS=$((FAILED_TESTS + 1))
        FAILED_TEST_LIST+=("2: 滑动窗口")
    fi
else
    echo "❌ 测试用例2: JAR文件不存在" | tee -a $RESULT_FILE
    FAILED_TESTS=$((FAILED_TESTS + 1))
    FAILED_TEST_LIST+=("2: 滑动窗口 (JAR不存在)")
fi

# 其他测试用例 (3-20) - 模拟执行
for i in {3..20}; do
    case $i in
        3) run_test_case $i "状态管理 - ValueState" "./run-test-case-$i.sh" ;;
        4) run_test_case $i "Kafka连接器" "./run-test-case-$i.sh" ;;
        5) run_test_case $i "批处理 - Join操作" "./run-test-case-$i.sh" ;;
        6) run_test_case $i "CEP复杂事件处理" "./run-test-case-$i.sh" ;;
        7) run_test_case $i "检查点和故障恢复" "./run-test-case-$i.sh" ;;
        8) run_test_case $i "自定义Source和Sink" "./run-test-case-$i.sh" ;;
        9) run_test_case $i "Table API和SQL" "./run-test-case-$i.sh" ;;
        10) run_test_case $i "流批一体化" "./run-test-case-$i.sh" ;;
        11) run_test_case $i "侧输出流" "./run-test-case-$i.sh" ;;
        12) run_test_case $i "广播状态" "./run-test-case-$i.sh" ;;
        13) run_test_case $i "异步I/O操作" "./run-test-case-$i.sh" ;;
        14) run_test_case $i "水印和延迟数据" "./run-test-case-$i.sh" ;;
        15) run_test_case $i "自定义分区器" "./run-test-case-$i.sh" ;;
        16) run_test_case $i "流表转换" "./run-test-case-$i.sh" ;;
        17) run_test_case $i "多流Join" "./run-test-case-$i.sh" ;;
        18) run_test_case $i "资源配置和性能调优" "./run-test-case-$i.sh" ;;
        19) run_test_case $i "容器化部署" "./run-test-case-$i.sh" ;;
        20) run_test_case $i "端到端集成测试" "./run-test-case-$i.sh" ;;
    esac
done

# 计算总耗时
END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

# 输出测试总结
echo "" | tee -a $RESULT_FILE
echo "=========================================="
echo "           测试执行完成"
echo "=========================================="
echo "结束时间: $(date)" | tee -a $RESULT_FILE
echo "总耗时: ${TOTAL_DURATION}秒" | tee -a $RESULT_FILE
echo "总测试用例: $TOTAL_TESTS" | tee -a $RESULT_FILE
echo "通过: $PASSED_TESTS" | tee -a $RESULT_FILE
echo "失败: $FAILED_TESTS" | tee -a $RESULT_FILE

if [ $FAILED_TESTS -gt 0 ]; then
    echo "" | tee -a $RESULT_FILE
    echo "失败的测试用例:" | tee -a $RESULT_FILE
    for failed_test in "${FAILED_TEST_LIST[@]}"; do
        echo "  - $failed_test" | tee -a $RESULT_FILE
    done
fi

# 计算成功率
SUCCESS_RATE=$((PASSED_TESTS * 100 / TOTAL_TESTS))
echo "" | tee -a $RESULT_FILE
echo "成功率: ${SUCCESS_RATE}%" | tee -a $RESULT_FILE

# 生成测试报告
echo "生成测试报告..."
./generate-test-report.sh $RESULT_FILE

# 清理测试环境
echo "清理测试环境..."
./cleanup-test-env.sh

if [ $FAILED_TESTS -eq 0 ]; then
    echo "🎉 所有测试用例执行成功！"
    exit 0
else
    echo "⚠️  有 $FAILED_TESTS 个测试用例失败"
    exit 1
fi