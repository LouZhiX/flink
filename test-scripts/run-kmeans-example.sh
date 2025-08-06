#!/bin/bash

# Flink KMeans 聚类算法测试脚本
# 适用于YARN集群环境，使用腾讯云COS存储

set -e

echo "=========================================="
echo "    Flink KMeans 聚类算法测试"
echo "=========================================="

# 配置参数
COS_BUCKET="cosn://junglelou-1258469122"
INPUT_FILE="$COS_BUCKET/test.txt"
OUTPUT_FILE="$COS_BUCKET/res_kmeans.txt"
FLINK_HOME=${FLINK_HOME:-"/usr/local/service/flink"}
KMEANS_JAR="$FLINK_HOME/examples/batch/KMeans.jar"

# 检查环境
echo "检查环境配置..."
if [ ! -f "$KMEANS_JAR" ]; then
    echo "错误: KMeans JAR文件不存在: $KMEANS_JAR"
    exit 1
fi

if ! command -v hadoop &> /dev/null; then
    echo "错误: hadoop命令不可用，请确保Hadoop已正确安装配置"
    exit 1
fi

echo "✓ 环境检查通过"

# 1. 准备测试数据
echo ""
echo "=== 1. 准备测试数据 ==="
cat > /tmp/kmeans_test_data.txt << 'EOF'
# 聚类1: 中心约在 (2, 2)
1.2 1.8
1.8 2.3
2.1 1.9
2.5 2.2
1.9 2.1
2.3 1.7
1.6 2.4
2.0 2.0
1.7 1.6
2.4 2.3
1.5 2.2
2.2 1.8
1.9 2.5
2.1 2.1
1.8 1.9
2.6 2.0
1.4 2.1
2.3 2.4
2.0 1.5
1.7 2.2

# 聚类2: 中心约在 (8, 8)
7.8 8.1
8.2 7.9
8.1 8.3
7.9 8.0
8.3 8.2
8.0 7.8
7.7 8.4
8.4 8.1
7.9 7.9
8.1 8.2
8.2 8.0
7.8 8.3
8.0 8.1
8.3 7.8
7.6 8.2
8.5 8.0
8.1 7.7
7.9 8.4
8.2 8.3
8.0 8.0

# 聚类3: 中心约在 (5, 1)
4.8 1.2
5.1 0.9
5.0 1.3
4.9 1.1
5.2 1.0
4.7 1.4
5.3 1.2
4.6 0.8
5.1 1.5
4.8 1.1
5.0 0.9
4.9 1.3
5.2 1.1
4.7 1.0
5.1 1.4
4.8 1.2
5.3 0.9
4.6 1.3
5.0 1.1
4.9 1.0

# 一些离群点
3.0 4.0
6.5 3.5
1.0 5.0
7.0 2.0
3.5 6.0
6.0 4.5
2.5 5.5
6.8 3.2
EOF

# 过滤掉注释行，只保留数据
grep -v '^#' /tmp/kmeans_test_data.txt | grep -v '^$' > /tmp/kmeans_clean_data.txt

echo "✓ 测试数据已准备完成 ($(wc -l < /tmp/kmeans_clean_data.txt) 个数据点)"
echo "前5行数据预览:"
head -5 /tmp/kmeans_clean_data.txt

# 2. 上传数据到COS
echo ""
echo "=== 2. 上传数据到COS ==="
echo "上传到: $INPUT_FILE"

# 删除已存在的文件
hadoop fs -rm -f $INPUT_FILE 2>/dev/null || true

# 上传新文件
if hadoop fs -put /tmp/kmeans_clean_data.txt $INPUT_FILE; then
    echo "✓ 数据上传成功"
else
    echo "✗ 数据上传失败"
    exit 1
fi

# 验证上传
echo "验证上传的文件:"
hadoop fs -ls $INPUT_FILE
echo "文件内容前5行:"
hadoop fs -cat $INPUT_FILE | head -5

# 3. 清理之前的输出
echo ""
echo "=== 3. 清理之前的输出 ==="
hadoop fs -rm -r -f $OUTPUT_FILE 2>/dev/null || true
echo "✓ 输出目录已清理"

# 4. 运行KMeans算法
echo ""
echo "=== 4. 运行KMeans聚类算法 ==="
echo "执行命令:"
echo "flink/bin/flink run -m yarn-cluster \\"
echo "  $KMEANS_JAR \\"
echo "  --input $INPUT_FILE \\"
echo "  --output $OUTPUT_FILE \\"
echo "  --iterations 20 \\"
echo "  --k 3"

echo ""
echo "开始执行..."
START_TIME=$(date +%s)

# 执行KMeans算法
if $FLINK_HOME/bin/flink run -m yarn-cluster \
    $KMEANS_JAR \
    --input $INPUT_FILE \
    --output $OUTPUT_FILE \
    --iterations 20 \
    --k 3; then
    
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo "✓ KMeans算法执行成功 (耗时: ${DURATION}秒)"
else
    echo "✗ KMeans算法执行失败"
    exit 1
fi

# 5. 检查和验证结果
echo ""
echo "=== 5. 检查结果 ==="

# 等待一下确保文件写入完成
sleep 5

echo "检查输出文件:"
if hadoop fs -test -e $OUTPUT_FILE; then
    echo "✓ 输出文件存在"
    
    echo ""
    echo "输出文件详情:"
    hadoop fs -ls $OUTPUT_FILE
    
    echo ""
    echo "聚类中心结果:"
    echo "格式: 聚类ID X坐标 Y坐标"
    echo "------------------------"
    hadoop fs -cat $OUTPUT_FILE
    
    # 保存结果到本地
    hadoop fs -cat $OUTPUT_FILE > /tmp/kmeans_results.txt
    echo ""
    echo "结果已保存到: /tmp/kmeans_results.txt"
    
else
    echo "✗ 输出文件不存在"
    exit 1
fi

# 6. 结果分析
echo ""
echo "=== 6. 结果分析 ==="
echo "预期聚类中心:"
echo "聚类1: 约 (2.0, 2.0) - 左下角数据群"
echo "聚类2: 约 (8.0, 8.0) - 右上角数据群"  
echo "聚类3: 约 (5.0, 1.0) - 中下方数据群"

echo ""
echo "实际聚类中心:"
cat /tmp/kmeans_results.txt

# 简单的结果验证
echo ""
echo "结果验证:"
CLUSTER_COUNT=$(wc -l < /tmp/kmeans_results.txt)
if [ $CLUSTER_COUNT -eq 3 ]; then
    echo "✓ 聚类数量正确: $CLUSTER_COUNT 个聚类中心"
else
    echo "✗ 聚类数量异常: $CLUSTER_COUNT 个聚类中心 (预期3个)"
fi

# 检查坐标范围合理性
echo "坐标范围检查:"
awk '{
    if ($2 >= 1 && $2 <= 3 && $3 >= 1 && $3 <= 3) print "✓ 聚类" $1 ": (" $2 "," $3 ") - 可能对应左下角群"
    else if ($2 >= 7 && $2 <= 9 && $3 >= 7 && $3 <= 9) print "✓ 聚类" $1 ": (" $2 "," $3 ") - 可能对应右上角群"
    else if ($2 >= 4 && $2 <= 6 && $3 >= 0 && $3 <= 2) print "✓ 聚类" $1 ": (" $2 "," $3 ") - 可能对应中下方群"
    else print "? 聚类" $1 ": (" $2 "," $3 ") - 位置异常"
}' /tmp/kmeans_results.txt

# 7. 清理临时文件
echo ""
echo "=== 7. 清理临时文件 ==="
rm -f /tmp/kmeans_test_data.txt /tmp/kmeans_clean_data.txt
echo "✓ 临时文件已清理"

# 8. 总结
echo ""
echo "=========================================="
echo "           KMeans测试完成"
echo "=========================================="
echo "输入文件: $INPUT_FILE"
echo "输出文件: $OUTPUT_FILE"
echo "本地结果: /tmp/kmeans_results.txt"
echo "执行时间: ${DURATION}秒"
echo ""
echo "可以使用以下命令查看详细结果:"
echo "hadoop fs -cat $OUTPUT_FILE"
echo ""
echo "如需可视化结果，请参考 kmeans-example.md 中的Python脚本"

# 提供下一步建议
echo ""
echo "建议的下一步操作:"
echo "1. 使用不同的k值测试聚类效果"
echo "2. 调整迭代次数观察收敛性"
echo "3. 使用更大的数据集测试性能"
echo "4. 尝试不同的YARN资源配置"