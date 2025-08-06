#!/bin/bash

# Flink测试环境清理脚本

set -e

echo "=== 清理Flink测试环境 ==="

# 停止所有运行的Flink作业
echo "停止运行中的Flink作业..."
if [ -n "$FLINK_HOME" ] && [ -f "$FLINK_HOME/bin/flink" ]; then
    # 获取所有运行的作业ID
    JOB_IDS=$($FLINK_HOME/bin/flink list 2>/dev/null | grep -E "^[a-f0-9-]+" | awk '{print $1}' || true)
    
    if [ -n "$JOB_IDS" ]; then
        for job_id in $JOB_IDS; do
            echo "取消作业: $job_id"
            $FLINK_HOME/bin/flink cancel $job_id || true
        done
    else
        echo "没有运行中的作业需要停止"
    fi
else
    echo "Flink未安装或FLINK_HOME未设置，跳过作业清理"
fi

# 停止Flink集群
echo "停止Flink集群..."
if [ -n "$FLINK_HOME" ] && [ -f "$FLINK_HOME/bin/stop-cluster.sh" ]; then
    $FLINK_HOME/bin/stop-cluster.sh || true
    echo "✓ Flink集群已停止"
else
    echo "跳过Flink集群停止"
fi

# 清理测试数据文件
echo "清理测试数据文件..."
rm -rf /tmp/flink-test-data/* 2>/dev/null || true
rm -rf /tmp/flink-test-output/* 2>/dev/null || true
rm -rf /tmp/flink-checkpoints/* 2>/dev/null || true

echo "✓ 测试数据文件已清理"

# 清理临时日志文件
echo "清理临时日志文件..."
rm -f /tmp/flink-*.log 2>/dev/null || true
rm -f /tmp/*.out 2>/dev/null || true

# 清理编译产生的文件
echo "清理编译文件..."
if [ -d "java-examples/target" ]; then
    rm -rf java-examples/target
    echo "✓ Java编译文件已清理"
fi

# 清理Docker容器 (如果有的话)
echo "清理Docker容器..."
docker ps -q --filter "name=flink" | xargs -r docker stop 2>/dev/null || true
docker ps -aq --filter "name=flink" | xargs -r docker rm 2>/dev/null || true
echo "✓ Docker容器已清理"

# 清理Kubernetes资源 (如果有的话)
echo "清理Kubernetes资源..."
kubectl delete deployment,service,pod -l app=flink --ignore-not-found=true 2>/dev/null || true
echo "✓ Kubernetes资源已清理"

# 显示清理后的磁盘空间
echo "清理后的磁盘使用情况:"
df -h /tmp 2>/dev/null || true

echo ""
echo "=== 测试环境清理完成 ==="
echo "✅ 所有测试资源已清理"
echo "✅ Flink集群已停止"
echo "✅ 临时文件已删除"
echo "✅ 容器和K8s资源已清理"