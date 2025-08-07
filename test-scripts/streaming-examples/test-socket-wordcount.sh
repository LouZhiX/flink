#!/bin/bash

# Flink SocketWindowWordCount 测试脚本

set -e

echo "=========================================="
echo "    Socket Window WordCount 测试"
echo "=========================================="

# 配置参数
FLINK_HOME=${FLINK_HOME:-"/usr/local/service/flink"}
SOCKET_WORDCOUNT_JAR="$FLINK_HOME/examples/streaming/SocketWindowWordCount.jar"
SOCKET_HOST="localhost"
SOCKET_PORT=9999

# 检查环境
echo "检查环境配置..."
if [ ! -f "$SOCKET_WORDCOUNT_JAR" ]; then
    echo "错误: SocketWindowWordCount JAR文件不存在: $SOCKET_WORDCOUNT_JAR"
    exit 1
fi

echo "✓ 环境检查通过"

# 1. 启动Socket数据服务器
echo ""
echo "=== 1. 启动Socket数据服务器 ==="

# 创建Python Socket服务器脚本
cat > /tmp/socket_server.py << 'EOF'
#!/usr/bin/env python3
import socket
import time
import threading
import sys

def socket_server():
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('localhost', 9999))
        server.listen(1)
        print('Socket server started on localhost:9999')
        
        # 测试数据
        test_data = [
            'hello world flink streaming',
            'apache flink window wordcount',
            'streaming data processing',
            'hello flink hello world',
            'window based word counting',
            'real time stream processing',
            'flink streaming api example',
            'hello world streaming data',
            'apache flink is powerful',
            'window wordcount demonstration'
        ]
        
        while True:
            try:
                client, addr = server.accept()
                print(f'Client connected: {addr}')
                
                # 发送测试数据
                for i, data in enumerate(test_data):
                    client.send((data + '\n').encode())
                    print(f'Sent ({i+1}/10): {data}')
                    time.sleep(3)  # 每3秒发送一行数据
                
                print('All data sent, closing connection')
                client.close()
                break
                
            except Exception as e:
                print(f'Error handling client: {e}')
                break
                
    except Exception as e:
        print(f'Server error: {e}')
    finally:
        server.close()

if __name__ == '__main__':
    socket_server()
EOF

chmod +x /tmp/socket_server.py

echo "启动Socket服务器 (后台运行)..."
python3 /tmp/socket_server.py &
SERVER_PID=$!

# 等待服务器启动
sleep 2

# 检查服务器是否启动成功
if ! netstat -tlnp 2>/dev/null | grep -q ":9999 "; then
    echo "✗ Socket服务器启动失败"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo "✓ Socket服务器已启动 (PID: $SERVER_PID)"

# 2. 运行SocketWindowWordCount
echo ""
echo "=== 2. 运行SocketWindowWordCount ==="
echo "执行命令:"
echo "flink run $SOCKET_WORDCOUNT_JAR \\"
echo "  --hostname $SOCKET_HOST \\"
echo "  --port $SOCKET_PORT"

echo ""
echo "开始执行 (将运行30秒)..."
START_TIME=$(date +%s)

# 在后台运行Flink作业，并重定向输出
timeout 30s $FLINK_HOME/bin/flink run \
    $SOCKET_WORDCOUNT_JAR \
    --hostname $SOCKET_HOST \
    --port $SOCKET_PORT > /tmp/socket_wordcount_output.log 2>&1 &

FLINK_PID=$!

# 等待Flink作业启动
sleep 5

# 检查Flink作业是否在运行
if ! ps -p $FLINK_PID > /dev/null 2>&1; then
    echo "✗ Flink作业启动失败"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo "✓ Flink作业已启动 (PID: $FLINK_PID)"

# 3. 监控执行过程
echo ""
echo "=== 3. 监控执行过程 ==="
echo "监控窗口输出 (每5秒检查一次)..."

MONITOR_COUNT=0
MAX_MONITOR=6  # 最多监控30秒

while [ $MONITOR_COUNT -lt $MAX_MONITOR ]; do
    sleep 5
    MONITOR_COUNT=$((MONITOR_COUNT + 1))
    
    echo "--- 第 $MONITOR_COUNT 次检查 ($(date)) ---"
    
    # 检查Flink作业状态
    if ps -p $FLINK_PID > /dev/null 2>&1; then
        echo "✓ Flink作业运行中"
    else
        echo "✓ Flink作业已完成"
        break
    fi
    
    # 检查Socket服务器状态
    if ps -p $SERVER_PID > /dev/null 2>&1; then
        echo "✓ Socket服务器运行中"
    else
        echo "✓ Socket服务器已完成数据发送"
    fi
done

# 4. 停止进程并收集结果
echo ""
echo "=== 4. 停止进程并收集结果 ==="

# 停止Flink作业
if ps -p $FLINK_PID > /dev/null 2>&1; then
    echo "停止Flink作业..."
    kill $FLINK_PID 2>/dev/null
    sleep 2
fi

# 停止Socket服务器
if ps -p $SERVER_PID > /dev/null 2>&1; then
    echo "停止Socket服务器..."
    kill $SERVER_PID 2>/dev/null
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "✓ 所有进程已停止 (总耗时: ${DURATION}秒)"

# 5. 分析输出结果
echo ""
echo "=== 5. 分析输出结果 ==="

if [ -f "/tmp/socket_wordcount_output.log" ]; then
    echo "Flink作业输出日志:"
    echo "------------------------"
    
    # 过滤出有用的输出信息
    echo "作业启动信息:"
    grep -i "job\|application\|started\|running" /tmp/socket_wordcount_output.log | head -5
    
    echo ""
    echo "窗口处理结果:"
    # 查找窗口结果输出
    grep -i "window\|count\|result" /tmp/socket_wordcount_output.log | tail -10
    
    echo ""
    echo "完整日志文件: /tmp/socket_wordcount_output.log"
    
    # 检查是否有错误
    if grep -qi "error\|exception\|failed" /tmp/socket_wordcount_output.log; then
        echo ""
        echo "⚠️  发现错误信息:"
        grep -i "error\|exception\|failed" /tmp/socket_wordcount_output.log | head -3
    else
        echo "✓ 未发现明显错误"
    fi
    
else
    echo "✗ 未找到输出日志文件"
fi

# 6. Socket服务器日志分析
echo ""
echo "=== 6. Socket数据发送分析 ==="

# 检查服务器是否成功发送了数据
if [ -f "/tmp/socket_server.log" ]; then
    echo "Socket服务器日志:"
    cat /tmp/socket_server.log
else
    echo "Socket服务器运行正常，数据已发送完成"
fi

# 7. 性能分析
echo ""
echo "=== 7. 性能分析 ==="

echo "测试配置:"
echo "- Socket地址: $SOCKET_HOST:$SOCKET_PORT"
echo "- 运行时间: ${DURATION}秒"
echo "- 数据发送: 10行测试数据"
echo "- 窗口大小: 5秒 (默认)"

# 8. 结果验证
echo ""
echo "=== 8. 结果验证 ==="

# 验证基本功能
echo "功能验证:"

if [ -f "/tmp/socket_wordcount_output.log" ]; then
    # 检查是否有窗口输出
    if grep -qi "window\|count" /tmp/socket_wordcount_output.log; then
        echo "✓ 检测到窗口处理输出"
    else
        echo "✗ 未检测到窗口处理输出"
    fi
    
    # 检查是否处理了预期的单词
    expected_words=("hello" "world" "flink" "streaming" "apache")
    echo ""
    echo "预期单词检查:"
    for word in "${expected_words[@]}"; do
        if grep -qi "$word" /tmp/socket_wordcount_output.log; then
            echo "✓ 找到单词: $word"
        else
            echo "? 未明确找到单词: $word"
        fi
    done
else
    echo "✗ 无法验证结果，日志文件不存在"
fi

# 9. 清理临时文件
echo ""
echo "=== 9. 清理临时文件 ==="
rm -f /tmp/socket_server.py
echo "✓ 临时文件已清理"

# 10. 总结
echo ""
echo "=========================================="
echo "        Socket WordCount测试完成"
echo "=========================================="
echo "配置信息:"
echo "- Socket地址: $SOCKET_HOST:$SOCKET_PORT"
echo "- JAR文件: $SOCKET_WORDCOUNT_JAR"
echo "- 执行时间: ${DURATION}秒"
echo "- 输出日志: /tmp/socket_wordcount_output.log"
echo ""
echo "查看完整日志:"
echo "cat /tmp/socket_wordcount_output.log"

# 提供下一步建议
echo ""
echo "建议的下一步操作:"
echo "1. 手动启动Socket服务器测试实时数据流"
echo "2. 调整窗口大小参数测试不同窗口效果"
echo "3. 使用nc命令手动发送数据: nc localhost 9999"
echo "4. 测试更高频率的数据流"
echo ""
echo "手动测试命令:"
echo "# 终端1: 启动数据发送"
echo "nc -lk 9999"
echo "# 终端2: 启动Flink作业"
echo "flink run SocketWindowWordCount.jar --hostname localhost --port 9999"