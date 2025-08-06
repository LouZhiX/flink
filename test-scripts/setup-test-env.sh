#!/bin/bash

# Flink测试环境设置脚本

set -e

echo "=== 设置Flink测试环境 ==="

# 检查Flink是否已安装
if [ -z "$FLINK_HOME" ]; then
    echo "错误: FLINK_HOME环境变量未设置"
    echo "请设置FLINK_HOME指向Flink安装目录"
    exit 1
fi

# 检查Flink版本
echo "检查Flink版本..."
$FLINK_HOME/bin/flink --version

# 创建测试数据目录
echo "创建测试数据目录..."
mkdir -p /tmp/flink-test-data
mkdir -p /tmp/flink-test-output
mkdir -p /tmp/flink-checkpoints

# 启动Flink集群
echo "启动Flink集群..."
$FLINK_HOME/bin/start-cluster.sh

# 等待集群启动
sleep 10

# 检查集群状态
echo "检查集群状态..."
$FLINK_HOME/bin/flink list

# 检查Web UI是否可访问
echo "检查Web UI (http://localhost:8081)..."
if curl -s http://localhost:8081 > /dev/null; then
    echo "✓ Web UI可访问"
else
    echo "✗ Web UI不可访问"
fi

# 设置测试数据
echo "准备测试数据..."

# 测试用例1的输入数据
cat > /tmp/flink-test-data/wordcount-input.txt << EOF
hello world
hello flink
flink streaming
world peace
streaming data processing
apache flink rocks
EOF

# 测试用例5的输入数据
cat > /tmp/flink-test-data/users.csv << EOF
1,Alice,Engineering
2,Bob,Marketing
3,Charlie,Engineering
4,Diana,Sales
EOF

cat > /tmp/flink-test-data/departments.csv << EOF
Engineering,100
Marketing,200
Sales,300
EOF

# 测试用例9的订单数据
cat > /tmp/flink-test-data/orders.csv << EOF
1,laptop,2,1499.99,2024-01-01 10:00:00
2,mouse,3,19.99,2024-01-01 10:05:00
3,laptop,1,1499.99,2024-01-01 10:10:00
4,keyboard,2,79.99,2024-01-01 10:15:00
EOF

echo "=== 测试环境设置完成 ==="
echo "Flink集群已启动，Web UI: http://localhost:8081"
echo "测试数据已准备完成"