#!/usr/bin/env python3
"""
AO3 批量文章爬虫

功能：从 work_id 列表批量爬取文章
"""

import urllib.request
import re
import json
import time


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}


def fetch_url(url, timeout=60):
    """获取URL内容"""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"获取失败 {url}: {e}")
        return None


def parse_chapters_from_full_work(html_text):
    """从HTML解析所有章节"""
    chapters = []

    chapter_areas = []
    for match in re.finditer(r'id="chapter-([0-9]+)"', html_text, re.IGNORECASE):
        chapter_id = match.group(1)
        start_pos = max(0, match.start() - 1000)
        end_pos = min(len(html_text), match.end() + 15000)
        section = html_text[start_pos:end_pos]
        chapter_areas.append((chapter_id, section))

    if chapter_areas:
        for chapter_id, section in chapter_areas:
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

            userstuff_match = re.search(r'<div[^>]*class="[^"]*userstuff[^"]*"[^>]*>(.*?)</div>', section, re.DOTALL | re.IGNORECASE)
            if userstuff_match:
                content = userstuff_match.group(1)
                clean_content = re.sub(r'<[^>]+>', '', content)
                clean_content = re.sub(r'\s+', ' ', clean_content).strip()
                clean_content = clean_content.replace(' . ', '.\n')

                chapters.append({
                    'chapter_id': chapter_id,
                    'chapter_title': chapter_title,
                    'content': clean_content
                })

    if not chapters:
        userstuff_matches = re.findall(r'<div[^>]*class="[^"]*userstuff[^"]*"[^>]*>(.*?)</div>', html_text, re.DOTALL | re.IGNORECASE)
        if userstuff_matches:
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


def fetch_work(work_id):
    """爬取单篇文章"""
    url = f"https://archiveofourown.org/works/{work_id}?view_full_work=true"

    html_text = fetch_url(url)
    if not html_text:
        return None

    title_match = re.search(r'<h2[^>]*class="[^"]*title[^"]*"[^>]*>(.*?)</h2>', html_text, re.DOTALL | re.IGNORECASE)
    title = title_match.group(1).strip() if title_match else "未知标题"
    title = re.sub(r'<[^>]+>', '', title).strip()

    author = "未知作者"
    author_patterns = [
        r'<a[^>]*class="[^"]*author[^"]*"[^>]*>([^<]+)</a>',
        r'<a[^>]*"rel="author"[^>]*>([^<]+)</a>',
        r'by\s*<a[^>]*>([^<]+)</a>',
    ]
    for pattern in author_patterns:
        author_match = re.search(pattern, html_text, re.IGNORECASE)
        if author_match:
            author = author_match.group(1).strip()
            break

    total_chapters = 1
    chapter_match = re.search(r'(\d+)/(\d+)', html_text)
    if chapter_match:
        total_chapters = int(chapter_match.group(2))

    chapters = parse_chapters_from_full_work(html_text)

    return {
        'work_id': work_id,
        'url': f"https://archiveofourown.org/works/{work_id}",
        'title': title,
        'author': author,
        'total_chapters': total_chapters,
        'chapters_fetched': len(chapters),
        'chapters': chapters
    }


def load_work_ids(file_path):
    """从文件加载 work_id 列表"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def batch_fetch(work_ids, delay=3):
    """批量爬取文章"""
    results = []
    failed = []

    print(f"开始批量爬取：共 {len(work_ids)} 篇文章")
    print("=" * 60)

    for i, work_id in enumerate(work_ids, 1):
        print(f"\n[{i}/{len(work_ids)}] 爬取 {work_id}...", end=' ', flush=True)

        try:
            result = fetch_work(work_id)
            if result:
                print(f"✓ {result['title']}")
                results.append(result)

                # 保存单篇文章
                output_file = f"ao3_{work_id}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
            else:
                print(f"✗ 失败")
                failed.append(work_id)

        except Exception as e:
            print(f"✗ 错误: {e}")
            failed.append(work_id)

        # 延迟，避免给服务器压力
        if i < len(work_ids):
            time.sleep(delay)

    print(f"\n{'=' * 60}")
    print(f"爬取完成！")
    print(f"  成功: {len(results)} 篇")
    print(f"  失败: {len(failed)} 篇")

    if failed:
        print(f"\n失败的 work_id:")
        for wid in failed:
            print(f"  {wid}")

    # 保存汇总
    summary = {
        'total': len(work_ids),
        'success': len(results),
        'failed': len(failed),
        'failed_ids': failed,
        'work_id_list': work_ids
    }

    with open('batch_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n汇总信息已保存到: batch_summary.json")

    return results


def main():
    # 加载 work_id 列表
    work_ids = load_work_ids('work_ids.txt')

    # 批量爬取（每篇之间延迟3秒）
    results = batch_fetch(work_ids, delay=3)


if __name__ == '__main__':
    main()
