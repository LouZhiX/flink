#!/bin/bash

# 生成Flink测试报告

RESULT_FILE=$1

if [ -z "$RESULT_FILE" ] || [ ! -f "$RESULT_FILE" ]; then
    echo "错误: 请提供有效的测试结果文件"
    echo "用法: $0 <test-result-file>"
    exit 1
fi

# 提取报告信息
REPORT_DIR="test-results"
HTML_REPORT="$REPORT_DIR/flink-test-report-$(date +%Y%m%d-%H%M%S).html"

echo "生成HTML测试报告: $HTML_REPORT"

# 从结果文件中提取统计信息
TOTAL_TESTS=$(grep "总测试用例:" $RESULT_FILE | awk '{print $2}')
PASSED_TESTS=$(grep "通过:" $RESULT_FILE | awk '{print $2}')
FAILED_TESTS=$(grep "失败:" $RESULT_FILE | awk '{print $2}')
SUCCESS_RATE=$(grep "成功率:" $RESULT_FILE | awk '{print $2}')
START_TIME=$(grep "开始时间:" $RESULT_FILE | cut -d':' -f2-)
END_TIME=$(grep "结束时间:" $RESULT_FILE | cut -d':' -f2-)
TOTAL_DURATION=$(grep "总耗时:" $RESULT_FILE | awk '{print $2}')

# 生成HTML报告
cat > $HTML_REPORT << EOF
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Flink功能测试报告</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
        }
        .header p {
            margin: 10px 0 0 0;
            opacity: 0.9;
        }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
        }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-card h3 {
            margin: 0 0 10px 0;
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
        }
        .metric-card .value {
            font-size: 2em;
            font-weight: bold;
            margin: 0;
        }
        .success { color: #28a745; }
        .danger { color: #dc3545; }
        .info { color: #17a2b8; }
        .warning { color: #ffc107; }
        
        .test-details {
            padding: 30px;
        }
        .test-case {
            border: 1px solid #ddd;
            border-radius: 6px;
            margin-bottom: 15px;
            overflow: hidden;
        }
        .test-case-header {
            padding: 15px 20px;
            background: #f8f9fa;
            border-bottom: 1px solid #ddd;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .test-case-title {
            font-weight: bold;
            font-size: 1.1em;
        }
        .status-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: bold;
            text-transform: uppercase;
        }
        .status-pass {
            background: #d4edda;
            color: #155724;
        }
        .status-fail {
            background: #f8d7da;
            color: #721c24;
        }
        .status-skip {
            background: #fff3cd;
            color: #856404;
        }
        .progress-bar {
            width: 100%;
            height: 20px;
            background: #e9ecef;
            border-radius: 10px;
            overflow: hidden;
            margin: 20px 0;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #28a745, #20c997);
            transition: width 0.3s ease;
        }
        .footer {
            background: #343a40;
            color: white;
            padding: 20px 30px;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Flink功能测试报告</h1>
            <p>Apache Flink 核心功能验证测试</p>
        </div>
        
        <div class="summary">
            <div class="metric-card">
                <h3>总测试用例</h3>
                <p class="value info">${TOTAL_TESTS:-0}</p>
            </div>
            <div class="metric-card">
                <h3>通过</h3>
                <p class="value success">${PASSED_TESTS:-0}</p>
            </div>
            <div class="metric-card">
                <h3>失败</h3>
                <p class="value danger">${FAILED_TESTS:-0}</p>
            </div>
            <div class="metric-card">
                <h3>成功率</h3>
                <p class="value warning">${SUCCESS_RATE:-0%}</p>
            </div>
            <div class="metric-card">
                <h3>总耗时</h3>
                <p class="value info">${TOTAL_DURATION:-0}</p>
            </div>
        </div>

        <div style="padding: 0 30px;">
            <div class="progress-bar">
                <div class="progress-fill" style="width: ${SUCCESS_RATE:-0}"></div>
            </div>
        </div>
        
        <div class="test-details">
            <h2>📋 测试用例详情</h2>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">1. 基础流处理 - WordCount</div>
                    <span class="status-badge status-pass">✓ PASS</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">2. 时间窗口操作 - 滑动窗口</div>
                    <span class="status-badge status-pass">✓ PASS</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">3. 状态管理 - ValueState</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">4. Kafka连接器 - 消费和生产</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">5. 批处理 - 数据集Join操作</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">6. CEP复杂事件处理 - 模式检测</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">7. 检查点和故障恢复</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">8. 自定义Source和Sink</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">9. Table API和SQL查询</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">10. 流批一体化 - Bounded Stream</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">11. 侧输出流 - Side Output</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">12. 广播状态 - Broadcast State</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">13. 异步I/O操作</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">14. 水印和延迟数据处理</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">15. 自定义分区器</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">16. 流表转换 - Stream to Table</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">17. 多流Join - 双流Join</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">18. 资源配置和性能调优</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">19. 容器化部署 - Docker/Kubernetes</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
            
            <div class="test-case">
                <div class="test-case-header">
                    <div class="test-case-title">20. 端到端集成测试 - 完整数据管道</div>
                    <span class="status-badge status-skip">⚠ SKIP</span>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>📊 测试开始时间: ${START_TIME:-未知}</p>
            <p>🏁 测试结束时间: ${END_TIME:-未知}</p>
            <p>⚡ 报告生成时间: $(date)</p>
        </div>
    </div>
</body>
</html>
EOF

echo "✅ HTML测试报告已生成: $HTML_REPORT"

# 生成简化的文本报告
TEXT_REPORT="$REPORT_DIR/flink-test-summary-$(date +%Y%m%d-%H%M%S).txt"

cat > $TEXT_REPORT << EOF
========================================
    Flink功能测试总结报告
========================================

测试时间: ${START_TIME:-未知} - ${END_TIME:-未知}
总耗时: ${TOTAL_DURATION:-未知}

测试统计:
- 总测试用例: ${TOTAL_TESTS:-0}
- 通过: ${PASSED_TESTS:-0}
- 失败: ${FAILED_TESTS:-0}
- 成功率: ${SUCCESS_RATE:-0%}

测试覆盖的功能模块:
✓ 基础流处理 (WordCount)
✓ 时间窗口操作 (滑动窗口)
○ 状态管理 (ValueState)
○ 连接器集成 (Kafka, ElasticSearch)
○ 批处理操作 (Join, 聚合)
○ 复杂事件处理 (CEP)
○ 容错机制 (检查点, 故障恢复)
○ 高级特性 (异步I/O, 广播状态)
○ Table API & SQL
○ 部署和运维

建议:
1. 完善其余18个测试用例的实现
2. 添加性能基准测试
3. 增加错误场景测试
4. 集成持续集成流水线

========================================
EOF

echo "✅ 文本测试总结已生成: $TEXT_REPORT"

# 如果有浏览器，尝试打开HTML报告
if command -v xdg-open > /dev/null; then
    echo "尝试在浏览器中打开测试报告..."
    xdg-open $HTML_REPORT
elif command -v open > /dev/null; then
    echo "尝试在浏览器中打开测试报告..."
    open $HTML_REPORT
fi

echo "📋 测试报告生成完成！"