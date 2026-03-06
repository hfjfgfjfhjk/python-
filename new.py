import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
from datetime import datetime

# ================== 数据库配置 ==================
DB_NAME = 'movies.db'


def init_database():
    """初始化SQLite数据库，创建专用于列表页的表"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # 创建电影列表专用表（只包含列表页实际会有的字段）
    c.execute('''
        CREATE TABLE IF NOT EXISTS movie_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            category TEXT,
            actors TEXT,
            genre TEXT,
            score REAL,
            crawl_date TEXT
        )
    ''')

    conn.commit()
    conn.close()
    print(f"[系统] 数据库 {DB_NAME} 中的 movie_list 表初始化完成。")


# ================== 爬取设置 ==================
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

BASE_URL = "https://www.1905.com/mdb/film/list/o0d0p{}.html"


def crawl_list_page(page_num):
    """爬取指定页码的电影列表页"""
    url = BASE_URL.format(page_num)
    try:
        print(f"[爬虫] 正在抓取第 {page_num} 页: {url}")
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.encoding = 'utf-8'
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[错误] 爬取第 {page_num} 页失败: {e}")
        return None


def parse_list_page(html, page_num):
    """
    解析列表页，提取电影信息
    返回电影数据列表，每个电影包含：标题、类别、演员、类型、评分
    """
    movies = []
    soup = BeautifulSoup(html, 'html.parser')

    # 寻找电影条目块（基于页面结构，通常每个电影在一个 dl 或 div 中）
    # 方法：查找所有包含主演信息的 dl
    movie_blocks = soup.find_all('dl', class_=re.compile(r'film|movie'))
    if not movie_blocks:
        # 如果没有 dl，尝试找 div
        movie_blocks = soup.find_all('div', class_=re.compile(r'info|item'))

    if not movie_blocks:
        # 最后手段：查找所有电影标题链接，并向上找父容器
        title_links = soup.find_all('a', href=re.compile(r'/mdb/film/\d+/'))
        for link in title_links:
            parent = link.find_parent(['dl', 'div', 'li'])
            if parent:
                movie_blocks.append(parent)

    for block in movie_blocks:
        movie_data = {}

        # 提取标题
        title_tag = block.find('a', href=re.compile(r'/mdb/film/'))
        if title_tag:
            movie_data['title'] = title_tag.get_text(strip=True)
        else:
            strong_tag = block.find('strong')
            if strong_tag:
                movie_data['title'] = strong_tag.get_text(strip=True)
            else:
                continue  # 无标题则跳过

        # 提取评分
        score_tag = block.find('span', class_=re.compile(r'score|rating'))
        if score_tag:
            try:
                movie_data['score'] = float(score_tag.get_text(strip=True))
            except:
                pass
        else:
            # 尝试从文本中正则提取评分
            block_text = block.get_text()
            score_match = re.search(r'评分[：:]\s*(\d+(\.\d+)?)', block_text)
            if score_match:
                movie_data['score'] = float(score_match.group(1))

        # 提取主演
        block_text = block.get_text()
        actors_match = re.search(r'主演[：:]\s*(.+?)(?=\n|类型|$)', block_text)
        if actors_match:
            movie_data['actors'] = actors_match.group(1).strip()
        else:
            movie_data['actors'] = ''

        # 提取类型
        genre_match = re.search(r'类型[：:]\s*(.+?)(?=\n|主演|$)', block_text)
        if genre_match:
            movie_data['genre'] = genre_match.group(1).strip()
        else:
            movie_data['genre'] = ''

        # 类别固定为“电影列表”
        movie_data['category'] = '电影列表'
        movie_data['crawl_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        movies.append(movie_data)

    print(f"[解析] 第 {page_num} 页解析到 {len(movies)} 部电影")
    return movies


def save_movies_to_db(movies):
    """将电影数据保存到 movie_list 表"""
    if not movies:
        return 0

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    inserted = 0
    for movie in movies:
        try:
            # 检查是否已存在相同标题（简单去重）
            c.execute("SELECT id FROM movie_list WHERE title = ?", (movie.get('title', ''),))
            if c.fetchone():
                continue

            c.execute('''
                INSERT INTO movie_list 
                (title, category, actors, genre, score, crawl_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                movie.get('title', ''),
                movie.get('category', ''),
                movie.get('actors', ''),
                movie.get('genre', ''),
                movie.get('score', None),
                movie.get('crawl_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            ))
            inserted += 1
        except Exception as e:
            print(f"[错误] 插入数据失败: {e}, 电影: {movie.get('title')}")

    conn.commit()
    conn.close()
    return inserted


# ================== 主程序 ==================
def main():
    # 1. 初始化数据库（创建 movie_list 表）
    init_database()

    # 2. 设置爬取页数范围（根据需要调整，例如50页可超1000条）
    start_page = 1
    end_page = 50  # 假设每页30部，50页约1500条，考虑去重后可能仍超1000

    total_inserted = 0

    for page in range(start_page, end_page + 1):
        html = crawl_list_page(page)
        if not html:
            continue

        movies = parse_list_page(html, page)
        inserted = save_movies_to_db(movies)
        total_inserted += inserted
        print(f"[进度] 第{page}页插入 {inserted} 条，累计插入 {total_inserted} 条")

        time.sleep(1.5)  # 礼貌性延迟

    print(f"\n[完成] 总共插入 {total_inserted} 条记录到 movie_list 表。")

    # 验证总数
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM movie_list")
    final_count = c.fetchone()[0]
    print(f"[验证] movie_list 表现有记录数: {final_count}")
    conn.close()


if __name__ == "__main__":
    main()