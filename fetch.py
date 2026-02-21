#!/usr/bin/env python3
"""
AO3 单篇文章爬虫 - 优化版本
使用 view_full_work=true 一次性获取所有章节（无分页）

功能：爬取指定文章的标题、作者、全部章节正文
输出：JSON格式
"""

import urllib.request
import urllib.error
import json
import sys
import time
import re

# AO3 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}


def fetch_url(url, timeout=60):
    """使用urllib获取URL内容"""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode('utf-8', errors='ignore')
    except urllib.error.HTTPError as e:
        print(f"HTTP错误 {e.code}: {e.reason}")
        return None
    except urllib.error.URLError as e:
        print(f"URL错误: {e.reason}")
        return None
    except Exception as e:
        print(f"请求失败: {e}")
        return None


def parse_chapters_from_full_work(html_text):
    """
    从 view_full_work=true 的HTML中解析所有章节
    返回章节列表
    """
    chapters = []

    # 策略1: 查找所有 id="chapter-X" 的位置，然后在该位置附近找标题和 userstuff
    chapter_areas = []
    for match in re.finditer(r'id="chapter-([0-9]+)"', html_text, re.IGNORECASE):
        chapter_id = match.group(1)
        # 从这个位置往前找标题，往后找 userstuff
        start_pos = max(0, match.start() - 1000)
        end_pos = min(len(html_text), match.end() + 15000)
        section = html_text[start_pos:end_pos]
        chapter_areas.append((chapter_id, section))

    if chapter_areas:
        # 找到了 chapter-X 的标记，尝试匹配对应的标题和内容
        for chapter_id, section in chapter_areas:
            # 查找标题 - 尝试多种模式
            chapter_title = f'Chapter {chapter_id}'
            title_patterns = [
                r'<h3[^>]*>(.*?)</h3>',
                r'<h4[^>]*>(.*?)</h4>',
                r'<div[^>]*class="[^"]*chapter[^"]*"[^>]*>(.*?)</div>',
            ]
            for pattern in title_patterns:
                title_match = re.search(pattern, section, re.DOTALL | re.IGNORECASE)
                if title_match:
                    raw_title = title_match.group(1)
                    clean_title = re.sub(r'<[^>]+>', '', raw_title).strip()
                    if clean_title:
                        chapter_title = clean_title
                        break

            # 查找 userstuff
            userstuff_match = re.search(r'<div[^>]*class="[^"]*userstuff[^"]*"[^>]*>(.*?)</div>', section, re.DOTALL | re.IGNORECASE)
            if userstuff_match:
                content = userstuff_match.group(1)
                # 清理HTML标签
                clean_content = re.sub(r'<[^>]+>', '', content)
                # 清理多余空白
                clean_content = re.sub(r'\s+', ' ', clean_content).strip()
                # 简单换行处理
                clean_content = clean_content.replace(' . ', '.\n')

                chapters.append({
                    'chapter_id': chapter_id,
                    'chapter_title': chapter_title,
                    'content': clean_content
                })

    # 如果上面没找到，使用策略2: 直接查找所有 userstuff div（单章节或特殊格式）
    if not chapters:
        userstuff_matches = re.findall(r'<div[^>]*class="[^"]*userstuff[^"]*"[^>]*>(.*?)</div>', html_text, re.DOTALL | re.IGNORECASE)

        if userstuff_matches:
            # 通常第一个是正文，多个章节时会对应多个
            for i, content in enumerate(userstuff_matches, 1):
                clean_content = re.sub(r'<[^>]+>', '', content)
                clean_content = re.sub(r'\s+', ' ', clean_content).strip()
                clean_content = clean_content.replace(' . ', '.\n')

                chapters.append({
                    'chapter_id': str(i),
                    'chapter_title': f'Chapter {i}',
                    'content': clean_content
                })

    return chapters


def fetch_work(work_id, view_full_work=True):
    """
    爬取指定work_id的文章

    Args:
        work_id: AO3文章ID（数字或完整URL）
        view_full_work: 是否使用 view_full_work=true 参数（默认True）
    """
    # 处理输入：支持纯数字或完整URL
    if work_id.startswith('http'):
        if '/works/' in work_id:
            work_id = work_id.split('/works/')[1].split('/')[0]
        else:
            print(f"错误：无法从URL中提取work_id: {work_id}")
            return None

    # 构造URL
    if view_full_work:
        base_url = f"https://archiveofourown.org/works/{work_id}?view_full_work=true"
    else:
        base_url = f"https://archiveofourown.org/works/{work_id}"

    print(f"开始爬取: {base_url}")

    try:
        # 获取页面
        html_text = fetch_url(base_url)
        if not html_text:
            print("无法获取页面")
            return None

        print(f"  获取成功，页面大小: {len(html_text)} 字节")

        # 提取标题
        title_match = re.search(r'<h2[^>]*class="[^"]*title[^"]*"[^>]*>(.*?)</h2>', html_text, re.DOTALL | re.IGNORECASE)
        title = title_match.group(1).strip() if title_match else "未知标题"
        title = re.sub(r'<[^>]+>', '', title).strip()

        # 提取作者 - 尝试多种模式
        author = "未知作者"
        author_patterns = [
            r'<a[^>]*class="[^"]*author[^"]*"[^>]*>([^<]+)</a>',
            r'<a[^>]*rel="author"[^>]*>([^<]+)</a>',
            r'by\s*<a[^>]*>([^<]+)</a>',
        ]
        for pattern in author_patterns:
            author_match = re.search(pattern, html_text, re.IGNORECASE)
            if author_match:
                author = author_match.group(1).strip()
                break

        # 解析总章节数
        total_chapters = 1
        chapter_match = re.search(r'(\d+)/(\d+)', html_text)
        if chapter_match:
            total_chapters = int(chapter_match.group(2))

        # 解析所有章节
        print(f"  标题: {title}")
        print(f"  作者: {author}")
        print(f"  总章节数: {total_chapters}")
        print(f"  正在解析章节...")

        chapters = parse_chapters_from_full_work(html_text)

        print(f"  ✓ 提取到 {len(chapters)} 个章节")

        # 组装结果
        result = {
            'work_id': work_id,
            'url': f"https://archiveofourown.org/works/{work_id}",
            'title': title,
            'author': author,
            'total_chapters': total_chapters,
            'chapters_fetched': len(chapters),
            'chapters': chapters
        }

        return result

    except Exception as e:
        print(f"错误：{e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    if len(sys.argv) < 2:
        print("用法: python fetch_single_optimized.py <work_id> [--no-full-work]")
        print("示例: python fetch_single_optimized.py 123456")
        print("示例: python fetch_single_optimized.py https://archiveofourown.org/works/123456")
        sys.exit(1)

    work_id = sys.argv[1]
    view_full_work = '--no-full-work' not in sys.argv

    result = fetch_work(work_id, view_full_work)

    if result:
        # 输出JSON
        json_str = json.dumps(result, ensure_ascii=False, indent=2)
        print("\n" + "="*60)
        print("爬取完成！JSON输出：")
        print("="*60)
        print(json_str)

        # 同时保存到文件
        output_file = f"ao3_{work_id.replace('/', '_')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(json_str)
        print(f"\n已保存到: {output_file}")


if __name__ == '__main__':
    main()
