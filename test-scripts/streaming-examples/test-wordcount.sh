#!/bin/bash

# Flink WordCount 流处理测试脚本

set -e

echo "=========================================="
echo "    WordCount 流处理测试"
echo "=========================================="

# 配置参数
FLINK_HOME=${FLINK_HOME:-"/usr/local/service/flink"}
WORDCOUNT_JAR="$FLINK_HOME/examples/streaming/WordCount.jar"
INPUT_FILE="/tmp/wordcount-input.txt"
OUTPUT_DIR="/tmp/wordcount-output"

# 检查环境
echo "检查环境配置..."
if [ ! -f "$WORDCOUNT_JAR" ]; then
    echo "错误: WordCount JAR文件不存在: $WORDCOUNT_JAR"
    exit 1
fi

echo "✓ 环境检查通过"

# 1. 准备测试数据
echo ""
echo "=== 1. 准备测试数据 ==="
cat > $INPUT_FILE << 'EOF'
hello world flink streaming
apache flink is great for real time processing
streaming data processing with apache flink
real time analytics using flink streaming
flink wordcount example demonstrates basic streaming
hello flink hello world streaming data
apache flink provides powerful streaming capabilities
data processing in real time with low latency
flink streaming api makes development easy
hello world of streaming data processing
EOF

echo "✓ 测试数据已准备完成 ($(wc -l < $INPUT_FILE) 行)"
echo "输入数据预览:"
head -3 $INPUT_FILE

# 2. 清理之前的输出
echo ""
echo "=== 2. 清理之前的输出 ==="
rm -rf $OUTPUT_DIR
echo "✓ 输出目录已清理"

# 3. 运行WordCount流处理
echo ""
echo "=== 3. 运行WordCount流处理 ==="
echo "执行命令:"
echo "flink run $WORDCOUNT_JAR \\"
echo "  --input file://$INPUT_FILE \\"
echo "  --output file://$OUTPUT_DIR"

echo ""
echo "开始执行..."
START_TIME=$(date +%s)

# 执行WordCount
if $FLINK_HOME/bin/flink run \
    $WORDCOUNT_JAR \
    --input file://$INPUT_FILE \
    --output file://$OUTPUT_DIR; then
    
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo "✓ WordCount执行成功 (耗时: ${DURATION}秒)"
else
    echo "✗ WordCount执行失败"
    exit 1
fi

# 4. 检查和验证结果
echo ""
echo "=== 4. 检查结果 ==="

# 等待一下确保文件写入完成
sleep 3

echo "检查输出目录:"
if [ -d "$OUTPUT_DIR" ]; then
    echo "✓ 输出目录存在"
    
    echo ""
    echo "输出文件列表:"
    ls -la $OUTPUT_DIR
    
    # 查找结果文件
    RESULT_FILES=$(find $OUTPUT_DIR -name "*.txt" -o -name "*" -type f | head -10)
    
    if [ -n "$RESULT_FILES" ]; then
        echo ""
        echo "WordCount结果 (前20行):"
        echo "格式: (单词,计数)"
        echo "------------------------"
        
        # 合并所有结果文件并显示
        cat $OUTPUT_DIR/* | head -20
        
        # 统计结果
        TOTAL_WORDS=$(cat $OUTPUT_DIR/* | wc -l)
        echo ""
        echo "统计信息:"
        echo "- 不同单词总数: $TOTAL_WORDS"
        
        # 显示出现频率最高的单词
        echo "- 出现频率最高的前5个单词:"
        cat $OUTPUT_DIR/* | sort -t',' -k2 -nr | head -5
        
        # 保存结果到本地
        cat $OUTPUT_DIR/* > /tmp/wordcount_results.txt
        echo "- 完整结果已保存到: /tmp/wordcount_results.txt"
        
    else
        echo "✗ 未找到结果文件"
        exit 1
    fi
else
    echo "✗ 输出目录不存在"
    exit 1
fi

# 5. 结果验证
echo ""
echo "=== 5. 结果验证 ==="

# 验证预期单词是否存在
expected_words=("hello" "world" "flink" "streaming" "apache" "data" "processing")
echo "验证预期单词是否存在:"

for word in "${expected_words[@]}"; do
    if grep -q "($word," /tmp/wordcount_results.txt; then
        count=$(grep "($word," /tmp/wordcount_results.txt | sed 's/.*,\([0-9]*\)).*/\1/')
        echo "✓ 找到单词: $word (出现 $count 次)"
    else
        echo "✗ 未找到单词: $word"
    fi
done

# 验证结果格式
echo ""
echo "验证输出格式:"
if grep -q "^(" /tmp/wordcount_results.txt && grep -q ")$" /tmp/wordcount_results.txt; then
    echo "✓ 输出格式正确: (单词,计数)"
else
    echo "✗ 输出格式异常"
fi

# 6. 性能分析
echo ""
echo "=== 6. 性能分析 ==="
INPUT_SIZE=$(wc -c < $INPUT_FILE)
WORD_COUNT=$(wc -w < $INPUT_FILE)
UNIQUE_WORDS=$TOTAL_WORDS

echo "性能指标:"
echo "- 输入文件大小: $INPUT_SIZE 字节"
echo "- 总单词数: $WORD_COUNT"
echo "- 不同单词数: $UNIQUE_WORDS"
echo "- 执行时间: ${DURATION}秒"
echo "- 处理速度: $((WORD_COUNT / DURATION)) 单词/秒"

# 7. 清理临时文件
echo ""
echo "=== 7. 清理 ==="
rm -f $INPUT_FILE
echo "✓ 临时文件已清理"

# 8. 总结
echo ""
echo "=========================================="
echo "           WordCount测试完成"
echo "=========================================="
echo "输入文件: $INPUT_FILE"
echo "输出目录: $OUTPUT_DIR"
echo "结果文件: /tmp/wordcount_results.txt"
echo "执行时间: ${DURATION}秒"
echo "处理单词: $WORD_COUNT 个"
echo "不同单词: $UNIQUE_WORDS 个"
echo ""
echo "可以使用以下命令查看完整结果:"
echo "cat /tmp/wordcount_results.txt"

# 提供下一步建议
echo ""
echo "建议的下一步操作:"
echo "1. 尝试更大的数据集测试性能"
echo "2. 调整并行度观察性能变化"
echo "3. 测试不同的输入格式"
echo "4. 与批处理WordCount对比性能"