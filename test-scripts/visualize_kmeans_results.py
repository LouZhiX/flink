#!/usr/bin/env python3
"""
Flink KMeans 聚类结果可视化脚本
用于分析和展示KMeans算法的聚类效果

使用方法:
python3 visualize_kmeans_results.py [data_file] [centers_file]
"""

import numpy as np
import matplotlib.pyplot as plt
import argparse
import sys
import os
from sklearn.metrics import silhouette_score
from scipy.spatial.distance import cdist

def load_data(data_file):
    """加载数据点"""
    try:
        data = np.loadtxt(data_file)
        if data.shape[1] != 2:
            raise ValueError(f"数据文件应该包含2列坐标，实际包含{data.shape[1]}列")
        return data
    except Exception as e:
        print(f"错误: 无法加载数据文件 {data_file}: {e}")
        sys.exit(1)

def load_centers(centers_file):
    """加载聚类中心"""
    try:
        # KMeans输出格式: cluster_id x_coord y_coord
        centers_data = np.loadtxt(centers_file)
        if len(centers_data.shape) == 1:
            centers_data = centers_data.reshape(1, -1)
        
        if centers_data.shape[1] < 3:
            raise ValueError(f"聚类中心文件格式错误，应该包含3列(ID,X,Y)，实际包含{centers_data.shape[1]}列")
        
        # 提取坐标 (第2列和第3列)
        centers = centers_data[:, 1:3]
        cluster_ids = centers_data[:, 0].astype(int)
        return centers, cluster_ids
    except Exception as e:
        print(f"错误: 无法加载聚类中心文件 {centers_file}: {e}")
        sys.exit(1)

def assign_points_to_clusters(data_points, centers):
    """将数据点分配到最近的聚类中心"""
    distances = cdist(data_points, centers)
    cluster_assignments = np.argmin(distances, axis=1)
    return cluster_assignments

def calculate_metrics(data_points, centers, cluster_assignments):
    """计算聚类质量指标"""
    metrics = {}
    
    # 计算轮廓系数
    if len(np.unique(cluster_assignments)) > 1:
        silhouette_avg = silhouette_score(data_points, cluster_assignments)
        metrics['silhouette_score'] = silhouette_avg
    else:
        metrics['silhouette_score'] = 0
    
    # 计算类内平方和 (WCSS)
    wcss = 0
    for i, center in enumerate(centers):
        cluster_points = data_points[cluster_assignments == i]
        if len(cluster_points) > 0:
            wcss += np.sum((cluster_points - center) ** 2)
    metrics['wcss'] = wcss
    
    # 计算每个聚类的大小
    cluster_sizes = []
    for i in range(len(centers)):
        size = np.sum(cluster_assignments == i)
        cluster_sizes.append(size)
    metrics['cluster_sizes'] = cluster_sizes
    
    return metrics

def create_visualization(data_points, centers, cluster_assignments, metrics, save_path=None):
    """创建可视化图表"""
    plt.figure(figsize=(12, 8))
    
    # 定义颜色
    colors = ['red', 'blue', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan']
    
    # 绘制数据点
    for i in range(len(centers)):
        cluster_points = data_points[cluster_assignments == i]
        if len(cluster_points) > 0:
            plt.scatter(cluster_points[:, 0], cluster_points[:, 1], 
                       c=colors[i % len(colors)], alpha=0.6, s=50,
                       label=f'聚类 {i} ({len(cluster_points)} 点)')
    
    # 绘制聚类中心
    plt.scatter(centers[:, 0], centers[:, 1], 
               c='black', s=200, marker='x', linewidths=3,
               label='聚类中心')
    
    # 添加聚类中心标注
    for i, center in enumerate(centers):
        plt.annotate(f'C{i}', (center[0], center[1]), 
                    xytext=(5, 5), textcoords='offset points',
                    fontsize=12, fontweight='bold')
    
    # 设置图表属性
    plt.xlabel('X 坐标', fontsize=12)
    plt.ylabel('Y 坐标', fontsize=12)
    plt.title('Flink KMeans 聚类结果可视化', fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3)
    
    # 添加质量指标文本
    metrics_text = f"""聚类质量指标:
轮廓系数: {metrics['silhouette_score']:.3f}
类内平方和: {metrics['wcss']:.2f}
数据点总数: {len(data_points)}
聚类数量: {len(centers)}"""
    
    plt.text(0.02, 0.98, metrics_text, transform=plt.gca().transAxes,
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"✓ 可视化图表已保存到: {save_path}")
    
    plt.show()

def print_analysis_report(data_points, centers, cluster_assignments, metrics):
    """打印分析报告"""
    print("\n" + "="*50)
    print("           KMeans 聚类分析报告")
    print("="*50)
    
    print(f"数据点总数: {len(data_points)}")
    print(f"聚类数量: {len(centers)}")
    print(f"轮廓系数: {metrics['silhouette_score']:.4f}")
    print(f"类内平方和 (WCSS): {metrics['wcss']:.2f}")
    
    print("\n聚类中心坐标:")
    for i, center in enumerate(centers):
        print(f"  聚类 {i}: ({center[0]:.3f}, {center[1]:.3f})")
    
    print("\n各聚类大小:")
    for i, size in enumerate(metrics['cluster_sizes']):
        percentage = (size / len(data_points)) * 100
        print(f"  聚类 {i}: {size} 个点 ({percentage:.1f}%)")
    
    # 轮廓系数解释
    print(f"\n轮廓系数解释:")
    if metrics['silhouette_score'] > 0.7:
        print("  > 0.7: 聚类效果很好")
    elif metrics['silhouette_score'] > 0.5:
        print("  > 0.5: 聚类效果较好")
    elif metrics['silhouette_score'] > 0.25:
        print("  > 0.25: 聚类效果一般")
    else:
        print("  <= 0.25: 聚类效果较差")
    
    # 数据分布分析
    print(f"\n数据分布分析:")
    x_range = np.max(data_points[:, 0]) - np.min(data_points[:, 0])
    y_range = np.max(data_points[:, 1]) - np.min(data_points[:, 1])
    print(f"  X坐标范围: {np.min(data_points[:, 0]):.2f} ~ {np.max(data_points[:, 0]):.2f} (跨度: {x_range:.2f})")
    print(f"  Y坐标范围: {np.min(data_points[:, 1]):.2f} ~ {np.max(data_points[:, 1]):.2f} (跨度: {y_range:.2f})")

def main():
    parser = argparse.ArgumentParser(description='可视化Flink KMeans聚类结果')
    parser.add_argument('data_file', nargs='?', default='/tmp/kmeans_clean_data.txt',
                       help='数据点文件路径 (默认: /tmp/kmeans_clean_data.txt)')
    parser.add_argument('centers_file', nargs='?', default='/tmp/kmeans_results.txt',
                       help='聚类中心文件路径 (默认: /tmp/kmeans_results.txt)')
    parser.add_argument('--save', '-s', help='保存图片的路径')
    parser.add_argument('--no-display', action='store_true', help='不显示图表，只保存')
    
    args = parser.parse_args()
    
    print("Flink KMeans 聚类结果可视化工具")
    print("-" * 40)
    
    # 检查文件是否存在
    if not os.path.exists(args.data_file):
        print(f"错误: 数据文件不存在: {args.data_file}")
        print("请先运行 run-kmeans-example.sh 生成测试数据")
        sys.exit(1)
    
    if not os.path.exists(args.centers_file):
        print(f"错误: 聚类中心文件不存在: {args.centers_file}")
        print("请先运行 run-kmeans-example.sh 执行KMeans算法")
        sys.exit(1)
    
    # 加载数据
    print(f"加载数据点: {args.data_file}")
    data_points = load_data(args.data_file)
    print(f"✓ 已加载 {len(data_points)} 个数据点")
    
    print(f"加载聚类中心: {args.centers_file}")
    centers, cluster_ids = load_centers(args.centers_file)
    print(f"✓ 已加载 {len(centers)} 个聚类中心")
    
    # 分配数据点到聚类
    print("分配数据点到聚类...")
    cluster_assignments = assign_points_to_clusters(data_points, centers)
    print("✓ 数据点分配完成")
    
    # 计算质量指标
    print("计算聚类质量指标...")
    metrics = calculate_metrics(data_points, centers, cluster_assignments)
    print("✓ 质量指标计算完成")
    
    # 打印分析报告
    print_analysis_report(data_points, centers, cluster_assignments, metrics)
    
    # 创建可视化
    if not args.no_display:
        print("\n创建可视化图表...")
        create_visualization(data_points, centers, cluster_assignments, metrics, args.save)
    elif args.save:
        print(f"\n保存图表到: {args.save}")
        create_visualization(data_points, centers, cluster_assignments, metrics, args.save)
        plt.close()  # 关闭图表不显示
    
    print("\n分析完成!")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断执行")
        sys.exit(1)
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)