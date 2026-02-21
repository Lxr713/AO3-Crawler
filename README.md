# AO3 Fanfiction Crawler

A lightweight, dependency-free crawler for [Archive of Our Own (AO3)](https://archiveofourown.org/), written in pure Python. Extracts fanfiction metadata and chapter content with minimal setup.

## Features

- 🚀 **Single Article Crawling** - Extract title, author, and all chapters
- 📖 **Multi-chapter Support** - Handles works with any number of chapters
- 🔍 **Search Page Extraction** - Build work_id lists from search/tag pages
- 📦 **Batch Crawling** - Process multiple works with built-in rate limiting
- 🎯 **No External Dependencies** - Pure Python standard library
- ⏱️  **Polite Crawling** - Configurable delays to respect server load

## Requirements

- Python 3.6+

No external packages required!

## Quick Start

### 1. Single Article

```bash
python3 fetch.py 79906886
```

Output: `ao3_79906886.json`

### 2. Batch Crawling

```bash
# Step 1: Extract work_ids from a search page
python3 extract.py

# Step 2: Batch crawl all works
python3 batch.py
```

## Usage

### Single Article Crawling

```bash
python3 fetch.py <work_id>
```

**Example:**

```bash
python3 fetch_single_optimized.py 79906886
python3 fetch_single_optimized.py https://archiveofourown.org/works/79906886
```

**Output Format:**

```json
{
  "work_id": "79906886",
  "url": "https://archiveofourown.org/works/79906886",
  "title": "【狂聪】一起住吧",
  "author": "Turmali",
  "total_chapters": 1,
  "chapters_fetched": 1,
  "chapters": [
    {
      "chapter_id": "1",
      "chapter_title": "Chapter 1",
      "content": "Full chapter text here..."
    }
  ]
}
```

### Extract Work IDs from Search Pages

Edit `extract_work_ids.py` to customize the search URL:

```python
# Default: All Chinese language works
search_url = 'https://archiveofourown.org/works/search?work_search[language_id]=zh&commit=Search'

# Example: Specific fandom
search_url = 'https://archiveofourown.org/tags/Rick%20and%20Morty/works'

# Example: Completed works, Chinese only
search_url = 'https://archiveofourown.org/works/search?work_search[complete]=1&work_search[language_id]=zh&commit=Search'
```

Run the extractor:

```bash
python3 extract_work_ids.py
```

Output: `work_ids_zh_test.txt` (one work_id per line)

### Batch Crawling

```bash
python3 batch_fetch.py
```

The script:
- Reads work_ids from `work_ids_zh_test.txt`
- Crawls each work with a 3-second delay
- Saves each work as `ao3_{work_id}.json`
- Generates `batch_summary.json` with statistics

**Adjust delay:** Edit `batch_fetch.py`:

```python
results = batch_fetch(work_ids, delay=3)  # 3 seconds between requests
```

## Scripts

| Script | Purpose |
|--------|---------|
| `fetch.py` | Single article crawler |
| `extract.py` | Extract work_ids from search pages |
| `batch.py` | Batch crawl multiple works |

## Technical Details

### view_full_work=true Optimization

Uses AO3's `?view_full_work=true` parameter to fetch all chapters in a single request, eliminating the need for pagination logic.

### HTML Parsing

Uses regex-based parsing (pure Python standard library) to extract:
- Title and author metadata
- Chapter headings and content
- Multi-chapter work structure

### Rate Limiting

Built-in delays protect both:
- AO3 server load (respectful crawling)
- Your network connection

## Testing

| Test Case | Status | Work ID |
|-----------|--------|---------|
| Single-chapter work | ✅ | 79906886 |
| Multi-chapter work | ✅ | 79779056 (2 chapters) |
| Work ID extraction | ✅ | Chinese search page |
| Batch crawling | ✅ | Ready to use |

## Limitations

- **Rate Limiting**: AO3 may throttle aggressive crawlers. Use reasonable delays.
- **HTML Structure**: Changes to AO3's HTML may break the parser.
- **Authentication**: Does not handle logged-in content (only public works).

## Disclaimer

This tool is for educational and personal use only. Please:

1. **Respect AO3's Terms of Service**
2. **Use reasonable crawl rates** (avoid overwhelming the server)
3. **Crawl responsibly** - consider using official APIs if available
4. **Attribute properly** if using data for publications

AO3 is a volunteer-run project. Be kind to their servers!

## Contributing

Issues and pull requests are welcome. Areas for improvement:

- [ ] Handle HTML structure changes gracefully
- [ ] Add retry logic for failed requests
- [ ] Support resume/checkpoint for large batches
- [ ] Export to CSV/SQLite formats
- [ ] Add more metadata fields (tags, kudos, bookmarks)

## License

MIT License - feel free to use, modify, and distribute.

## Acknowledgments

Built for research purposes to study fanfiction content patterns. Respecting the creative work of both AO3 volunteers and fanfiction authors.
