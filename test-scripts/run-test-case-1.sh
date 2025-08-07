#!/bin/bash

# 测试用例1: 基础流处理 - WordCount

set -e

echo "=== 执行测试用例1: WordCount ==="

# 检查输入文件
if [ ! -f /tmp/flink-test-data/wordcount-input.txt ]; then
    echo "错误: 输入文件不存在"
    exit 1
fi

# 清理之前的输出
rm -rf /tmp/flink-test-output/wordcount-output*

echo "输入数据:"
cat /tmp/flink-test-data/wordcount-input.txt

# 运行WordCount示例
echo "提交WordCount作业..."
$FLINK_HOME/bin/flink run \
    $FLINK_HOME/examples/streaming/WordCount.jar \
    --input file:///tmp/flink-test-data/wordcount-input.txt \
    --output file:///tmp/flink-test-output/wordcount-output

# 等待作业完成
sleep 5

# 检查输出结果
echo "输出结果:"
if [ -f /tmp/flink-test-output/wordcount-output ]; then
    cat /tmp/flink-test-output/wordcount-output
    echo "✓ 测试用例1执行成功"
else
    echo "✗ 输出文件不存在"
    exit 1
fi

# 验证结果正确性
expected_words=("hello" "world" "flink" "streaming" "peace" "data" "processing" "apache" "rocks")
for word in "${expected_words[@]}"; do
    if grep -q "$word" /tmp/flink-test-output/wordcount-output; then
        echo "✓ 找到单词: $word"
    else
        echo "✗ 未找到单词: $word"
    fi
done

echo "=== 测试用例1完成 ==="