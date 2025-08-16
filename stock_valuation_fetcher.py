import asyncio
import aiohttp
import os
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
from typing import List, Tuple, Dict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"valuation_fetch_{datetime.now().strftime('%Y%m%d')}.log"),  # 日志文件
        logging.StreamHandler()  # 控制台输出
    ]
)
logger = logging.getLogger(__name__)

# 配置参数
CONCURRENT_LIMIT = 10  # 并发请求限制
TIMEOUT = 15  # 超时时间(秒)
INPUT_FILE = "rank_above_90.txt"  # 股票代码文件
OUTPUT_FILE = f"valuation_results_{datetime.now().strftime('%Y%m%d')}.txt"  # 结果文件

def get_market_prefix(stock_code: str) -> str:
    """根据股票代码判断市场前缀"""
    if not stock_code or len(stock_code) != 6 or not stock_code.isdigit():
        return ""
        
    first_char = stock_code[0]
    if first_char == '6':
        return 'SHSE'
    elif first_char in ('0', '3'):
        return 'SZSE'
    else:
        return ""

def read_stock_codes(file_path: str) -> List[str]:
    """读取并验证股票代码"""
    if not os.path.exists(file_path):
        logger.error(f"股票代码文件不存在: {file_path}")
        return []
    
    stock_codes = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            code = line.strip()
            if code and len(code) == 6 and code.isdigit():
                stock_codes.append(code)
            else:
                logger.warning(f"无效股票代码(行{line_num}): {code} - 已跳过")
    
    logger.info(f"成功读取 {len(stock_codes)} 个有效股票代码")
    return stock_codes

async def fetch_valuation(
    session: aiohttp.ClientSession, 
    stock_code: str, 
    market_prefix: str,
    semaphore: asyncio.Semaphore
) -> Tuple[str, str, str]:
    """异步获取单个股票的估值信息"""
    async with semaphore:  # 控制并发数
        url = f"https://www.gurufocus.com/stock/{market_prefix}:{stock_code}/valuation"
        headers = {
            "Cookie": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        
        try:
            start_time = datetime.now()
            async with session.get(
                url, 
                headers=headers, 
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                ssl=False
            ) as response:
                # 记录请求耗时
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.debug(f"请求耗时 {elapsed:.2f}s - {market_prefix}:{stock_code} (状态码: {response.status})")
                
                if response.status != 200:
                    return stock_code, market_prefix, f"HTTP Error: {response.status}"
                
                # 解析HTML获取标题
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                title_tag = soup.title
                
                if not title_tag:
                    return stock_code, market_prefix, "No Title Found"
                
                title_text = title_tag.get_text(strip=True)
                
                # 估值关键词匹配
                valuation_keywords = [
                    "Significantly Overvalued",
                    "Modestly Overvalued",
                    "Fairly Valued",
                    "Modestly Undervalued",
                    "Significantly Undervalued",
                    "Possible Value Trap",
                    "Data Out of Date"
                ]
                
                for keyword in valuation_keywords:
                    if keyword in title_text:
                        logger.info(f"成功获取估值 - {market_prefix}:{stock_code} -> {keyword}")
                        return stock_code, market_prefix, keyword
                
                return stock_code, market_prefix, "Unknown Valuation"
                
        except asyncio.TimeoutError:
            logger.error(f"请求超时({TIMEOUT}s) - {market_prefix}:{stock_code}")
            return stock_code, market_prefix, f"Timeout after {TIMEOUT}s"
        except aiohttp.ClientError as e:
            logger.error(f"网络请求错误 - {market_prefix}:{stock_code}: {str(e)}")
            return stock_code, market_prefix, f"Network Error: {str(e)}"
        except Exception as e:
            logger.error(f"处理错误 - {market_prefix}:{stock_code}: {str(e)}", exc_info=True)
            return stock_code, market_prefix, f"Error: {str(e)}"

async def batch_fetch(stock_codes: List[str]) -> List[Tuple[str, str, str]]:
    """批量异步获取估值信息"""
    # 过滤无效代码并准备任务
    valid_tasks = []
    for code in stock_codes:
        market = get_market_prefix(code)
        if market:
            valid_tasks.append((code, market))
        else:
            logger.warning(f"跳过无效代码: {code} (无法识别市场)")
    
    if not valid_tasks:
        logger.warning("没有有效任务可执行")
        return []
    
    # 控制并发的信号量
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    
    # 创建异步会话
    async with aiohttp.ClientSession() as session:
        # 创建所有任务
        tasks = [
            fetch_valuation(session, code, market, semaphore)
            for code, market in valid_tasks
        ]
        
        # 带进度条执行任务
        results = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="处理进度"):
            result = await f
            results.append(result)
        
        return results

def save_results(results: List[Tuple[str, str, str]]):
    """保存结果到文件"""
    if not results:
        logger.warning("没有结果可保存")
        return
    
    # 按股票代码排序
    results.sort(key=lambda x: x[0])
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write("股票代码\t市场\t估值状态\n")
        f.write("=" * 50 + "\n")
        for code, market, valuation in results:
            f.write(f"{code}\t{market}\t{valuation}\n")
    
    logger.info(f"所有结果已保存到: {os.path.abspath(OUTPUT_FILE)}")

def main():
    logger.info("===== 开始股票估值信息获取程序 =====")
    
    # 读取股票代码
    stock_codes = read_stock_codes(INPUT_FILE)
    if not stock_codes:
        logger.error("没有可用的股票代码，程序退出")
        return
    
    # 异步批量获取
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        results = loop.run_until_complete(batch_fetch(stock_codes))
    except Exception as e:
        logger.critical(f"程序执行出错: {str(e)}", exc_info=True)
        return
    
    # 保存结果
    save_results(results)
    
    logger.info("===== 程序执行完毕 =====")

if __name__ == "__main__":
    main()
