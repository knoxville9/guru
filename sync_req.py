import asyncio
import aiohttp
import json
import logging
from typing import Tuple, Dict, Any
from collections import defaultdict
import sys
import os
from datetime import datetime

# 全局配置
MAX_WORKERS = 10  # 全局并发数控制
TIMEOUT = 10  # 请求超时时间(秒)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 全局请求计数器
request_counter = defaultdict(int)

def get_today_folder():
    """获取当天日期的文件夹名称，格式为YYYY-MM-DD"""
    today = datetime.now().strftime("%Y-%m-%d")
    folder_name = f"stock_data_{today}"
    
    # 创建文件夹（如果不存在）
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        logger.info(f"创建日期文件夹: {folder_name}")
    else:
        logger.info(f"使用已存在的日期文件夹: {folder_name}")
    
    return folder_name

async def query_gurufocus_history(
    session: aiohttp.ClientSession,
    stock_code: str, 
    market_prefix: str, 
    authorization: str, 
    cookie: str, 
    signature: str
) -> Tuple[str, Dict[str, Any], int, str]:
    """异步查询股票历史数据，带请求次数统计"""
    # 增加请求计数器
    request_counter[stock_code] += 1
    current_attempt = request_counter[stock_code]
    
    url = f"https://www.gurufocus.com/reader/_api/gf_rank/{market_prefix}{stock_code}?v=1.7.44"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9",
        "authorization": authorization,
        "priority": "u=1, i",
        "referer": "https://www.gurufocus.com/stocks/region/asia/china",
        "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
        "sec-ch-ua-arch": '""',
        "sec-ch-ua-bitness": '""',
        "sec-ch-ua-full-version": '""',
        "sec-ch-ua-full-version-list": "",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-platform-version": '""',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "signature": signature,
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }
    
    # 解析cookies
    cookies = {}
    try:
        cookies = dict([c.strip().split('=', 1) for c in cookie.split(';') if '=' in c])
    except Exception as e:
        logger.error(f"解析Cookie失败: {str(e)}, Cookie内容: {cookie}")
    
    try:
        async with session.get(
            url, 
            headers=headers, 
            cookies=cookies,
            ssl=False,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT)
        ) as response:
            status_code = response.status
            logger.debug(f"股票代码: {stock_code} ({market_prefix}) (第{current_attempt}次尝试), 响应状态码: {status_code}")
            
            if status_code == 200:
                try:
                    data = await response.json()
                    return stock_code, data, status_code, market_prefix
                except json.JSONDecodeError as e:
                    logger.error(f"股票代码: {stock_code} ({market_prefix}) (第{current_attempt}次尝试), 解析JSON失败: {str(e)}")
                    text = await response.text()
                    logger.debug(f"响应内容: {text[:500]}")
                    return stock_code, {"error": "JSON解析失败", "content": text[:500]}, status_code, market_prefix
            else:
                text = await response.text()
                logger.warning(f"股票代码: {stock_code} ({market_prefix}) (第{current_attempt}次尝试), 非200状态码: {status_code}, 响应内容: {text[:500]}")
                return stock_code, {"error": "非200状态码", "status_code": status_code, "content": text[:500]}, status_code, market_prefix
                
    except asyncio.TimeoutError:
        logger.error(f"股票代码: {stock_code} ({market_prefix}) (第{current_attempt}次尝试), 请求超时({TIMEOUT}秒)")
        return stock_code, {"error": "请求超时"}, 408, market_prefix
    except aiohttp.ClientError as e:
        logger.error(f"股票代码: {stock_code} ({market_prefix}) (第{current_attempt}次尝试), HTTP请求错误: {str(e)}")
        return stock_code, {"error": "HTTP请求错误", "details": str(e)}, 500, market_prefix
    except Exception as e:
        logger.error(f"股票代码: {stock_code} ({market_prefix}) (第{current_attempt}次尝试), 发生未知错误: {str(e)}", exc_info=True)
        return stock_code, {"error": "未知错误", "details": str(e)}, 500, market_prefix

def get_market_prefix(stock_code: str) -> str:
    """根据股票代码开头判断市场前缀"""
    if not stock_code:
        return ""
        
    first_char = stock_code[0]
    if first_char == '6':
        return 'SHSE:'
    elif first_char in ('0', '3'):
        return 'SZSE:'
    else:
        logger.warning(f"未知的股票代码格式: {stock_code}，默认使用SHSE:")
        return 'SHSE:'

def read_stock_codes(file_path: str) -> list:
    """从文件读取股票代码并去重"""
    if not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        raise FileNotFoundError(f"股票代码文件 {file_path} 不存在")
    
    stock_codes = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            code = line.strip()
            if code and code not in stock_codes:  # 去重
                stock_codes.append(code)
    
    logger.info(f"从文件 {file_path} 读取到 {len(stock_codes)} 个股票代码")
    return stock_codes

def save_rank_above_90_codes(codes, folder_path):
    """保存rank大于90的股票代码到TXT文件"""
    if not codes:
        logger.info("没有rank大于90的股票代码需要保存")
        return
        
    file_path = os.path.join(folder_path, "rank_above_90.txt")
    with open(file_path, 'w', encoding='utf-8') as f:
        for code in codes:
            f.write(f"{code}\n")
    logger.info(f"已生成rank>90的股票代码文件: {file_path} (共{len(codes)}个)")

async def batch_request_save_filtered(
    authorization: str, 
    cookie: str, 
    signature: str,
    code_file_path: str
):
    """从文件读取股票代码，批量异步请求并保存过滤后的数据，带进度提示"""
    # 获取当天日期文件夹
    today_folder = get_today_folder()
    
    # 从文件读取股票代码
    stock_codes = read_stock_codes(code_file_path)
    if not stock_codes:
        logger.warning("没有有效的股票代码可处理，程序退出")
        return
    
    # 用于保存rank>90的股票代码
    rank_above_90_codes = []
    
    # 构建任务列表
    tasks = []
    for code in stock_codes:
        # 自动判断市场前缀
        market_prefix = get_market_prefix(code)
        tasks.append((market_prefix, code))
    
    # 计算总任务数
    total_tasks = len(tasks)
    completed_tasks = 0
    
    logger.info(f"开始处理任务，共 {total_tasks} 个股票代码，并发数: {MAX_WORKERS}")
    
    # 限制并发数
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    
    async def bounded_task(market_prefix, stock_code):
        nonlocal completed_tasks
        try:
            result = await query_gurufocus_history(
                session, 
                stock_code, 
                market_prefix, 
                authorization, 
                cookie, 
                signature
            )
            return result
        finally:
            # 完成一个任务就更新进度
            completed_tasks += 1
            progress = (completed_tasks / total_tasks) * 100
            # 打印进度（覆盖当前行）
            sys.stdout.write(f"\r处理进度: {completed_tasks}/{total_tasks} ({progress:.2f}%) | 当前股票: {stock_code} ({market_prefix})")
            sys.stdout.flush()
    
    # 创建异步会话
    async with aiohttp.ClientSession() as session:
        # 创建所有任务
        all_tasks = [bounded_task(market_prefix, stock_code) for market_prefix, stock_code in tasks]
        
        # 等待所有任务完成
        results = await asyncio.gather(*all_tasks)
        
        # 换行，避免进度条被覆盖
        print()
        
        # 处理结果
        for stock_code, data, status_code, market_prefix in results:
            try:
                if status_code == 200:
                    rank = data.get('rank', None)
                    # 根据rank过滤
                    if rank is not None and rank < 90:
                        logger.info(f"股票代码: {stock_code} ({market_prefix}), rank={rank} < 90，不保存 (请求次数: {request_counter[stock_code]})")
                        continue
                    
                    # 如果rank>90，添加到列表中
                    if rank is not None and rank >= 90:
                        rank_above_90_codes.append(stock_code)
                    
                    # 保存数据到日期文件夹
                    filename = f"{stock_code}_{market_prefix.replace(':', '')}.json"
                    file_path = os.path.join(today_folder, filename)
                    
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    logger.info(f"已保存: {file_path}, 响应长度: {len(json.dumps(data))}, rank={rank} (请求次数: {request_counter[stock_code]})")
                else:
                    logger.warning(f"股票代码: {stock_code} ({market_prefix}), 处理失败，状态码: {status_code} (请求次数: {request_counter[stock_code]})")
            except Exception as e:
                logger.error(f"处理股票代码: {stock_code} ({market_prefix}) 时发生错误: {str(e)} (请求次数: {request_counter[stock_code]})", exc_info=True)
    
    # 保存rank>90的股票代码到TXT
    save_rank_above_90_codes(rank_above_90_codes, today_folder)
    
    # 打印总体统计信息
    logger.info("\n===== 任务统计 =====")
    logger.info(f"总任务数: {total_tasks}")
    logger.info(f"完成任务数: {completed_tasks}")
    logger.info(f"rank>90的股票数: {len(rank_above_90_codes)}")
    logger.info(f"数据保存目录: {os.path.abspath(today_folder)}")
    logger.info(f"平均请求次数: {sum(request_counter.values())/len(request_counter):.2f}")
    
    # 按市场统计
    sh_count = sum(1 for code in stock_codes if code.startswith('6'))
    sz_count = len(stock_codes) - sh_count
    logger.info(f"沪市(SHSE)股票数: {sh_count}")
    logger.info(f"深市(SZSE)股票数: {sz_count}")
    logger.info("====================")

def main():
    # 在main函数中直接设置参数，使用时只需修改这里的值
    # 股票代码文件路径
    code_file_path = "code.txt"
    
    # 以下参数需要替换为实际的值
    authorization = ""
    cookie = ""
    signature = ""

    
    try:
        asyncio.run(batch_request_save_filtered(
            authorization, 
            cookie, 
            signature,
            code_file_path
        ))
    except Exception as e:
        logger.critical(f"程序运行出错: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
