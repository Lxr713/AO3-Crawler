#!/usr/bin/env python3
"""
AO3 搜索页 work_id 提取脚本

功能：从 AO3 搜索页面提取所有文章的 work_id
支持分页爬取
"""

import urllib.request
import re
import time


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}


def fetch_page(url, timeout=60):
    """获取页面内容"""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"获取页面失败: {e}")
        return None


def extract_work_ids_from_html(html):
    """从HTML中提取所有 work_id"""
    work_links = re.findall(r'href="/works/([0-9]+)"', html)
    # 去重并排序
    return sorted(set(work_links), key=int)


def get_total_works(html):
    """获取总作品数"""
    match = re.search(r'(\d+,?\d*)\s*works found', html, re.IGNORECASE)
    return match.group(1) if match else None


def get_next_page_url(html, base_url):
    """获取下一页链接"""
    match = re.search(r'<a[^>]*rel="next"[^>]*href="([^"]+)"', html, re.IGNORECASE)
    if match:
        next_path = match.group(1)
        # 处理 &amp; 转义
        next_path = next_path.replace('&amp;', '&')
        # 构造完整URL
        if next_path.startswith('/'):
            return f"{base_url}{next_path}"
        else:
            return f"{base_url}/{next_path}"
    return None


def extract_all_work_ids(start_url, max_pages=None, delay=2):
    """
    从搜索页面提取所有 work_id

    Args:
        start_url: 搜索页面URL
        max_pages: 最多爬取多少页（None=全部）
        delay: 每页之间的延迟（秒）

    Returns:
        list: 所有 work_id
    """
    all_ids = []
    current_url = start_url
    page_num = 1

    print(f"开始提取 work_id: {start_url}")
    print("-" * 60)

    while current_url:
        print(f"\n第 {page_num} 页: {current_url}")

        html = fetch_page(current_url)
        if not html:
            print(f"  ❌ 获取失败，停止")
            break

        # 提取本页的 work_id
        page_ids = extract_work_ids_from_html(html)
        print(f"  ✓ 找到 {len(page_ids)} 个 work_id")
        all_ids.extend(page_ids)

        # 第一页显示总数
        if page_num == 1:
            total_works = get_total_works(html)
            if total_works:
                print(f"  总作品数: {total_works}")

        # 检查是否达到最大页数
        if max_pages and page_num >= max_pages:
            print(f"  已达到最大页数 ({max_pages})，停止")
            break

        # 获取下一页
        current_url = get_next_page_url(html, 'https://archiveofourown.org')
        if current_url:
            print(f"  → 下一页: {current_url.split('?')[0]}?...")
        else:
            print(f"  没有更多页面")
            break

        page_num += 1

        # 延迟，避免给服务器压力
        print(f"  等待 {delay} 秒...")
        time.sleep(delay)

    print(f"\n{'=' * 60}")
    print(f"提取完成！共 {len(all_ids)} 个 work_id")
    print(f"{'=' * 60}")

    return all_ids


def main():
    # 中文作品搜索
    search_url = 'https://archiveofourown.org/works/search?work_search%5Blanguage_id%5D=zh&commit=Search'

    # 测试：只提取前 2 页
    print("=" * 60)
    print("⚠️  测试模式：只提取前 2 页")
    print("=" * 60)

    work_ids = extract_all_work_ids(search_url, max_pages=2, delay=2)

    # 保存到文件
    output_file = "work_ids.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        for wid in work_ids:
            f.write(f"{wid}\n")

    print(f"\n✅ work_id 已保存到: {output_file}")
    print(f"前 10 个:")
    for wid in work_ids[:10]:
        print(f"  {wid}")
    print(f"  ...")


if __name__ == '__main__':
    main()
