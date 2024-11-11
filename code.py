import streamlit as st
import pandas as pd
import yfinance as yf
import sqlite3
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import logging
import random
import threading

# 配置日志
logging.basicConfig(
    filename='options_viewer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 添加一个全局变量用于yfinance连接状态
YFINANCE_STATUS = {
    'is_connected': True,
    'last_check': datetime.now(),
    'lock': threading.Lock()
}

def check_yfinance_connectivity():
    """
    检查yfinance API是否可访问
    返回True表示连接成功，False表示连接失败
    """
    try:
        # 尝试获取少量数据作为测试
        test_ticker = yf.Ticker('AAPL')
        _ = test_ticker.info
        return True
    except Exception as e:
        logging.error(f"YFinance connectivity check failed: {str(e)}")
        return False

def update_yfinance_status():
    """
    启动后台线程，每分钟检查一次yfinance连接状态
    """
    global YFINANCE_STATUS
    with YFINANCE_STATUS['lock']:
        YFINANCE_STATUS['is_connected'] = check_yfinance_connectivity()
        YFINANCE_STATUS['last_check'] = datetime.now()

def start_connectivity_checker():
    """
    Start background thread to check yfinance connectivity every minute
    """
    def check_periodically():
        while True:
            update_yfinance_status()
            time.sleep(60)  # Wait for 1 minute

    checker_thread = threading.Thread(target=check_periodically, daemon=True)
    checker_thread.start()

def get_current_time():
    """获取上海时区的当前时间"""
    return datetime.now(pytz.timezone('Asia/Shanghai'))

def compare_options_data(data1, data2):
    """
    比较两个期权链数据是否一致
    返回布尔值和不一致的字段列表
    """
    if data1 is None or data2 is None:
        return False, ["数据不完整"]
    
    # 确保两个数据框具有相同的列
    columns_to_compare = [
        'strike', 'lastPrice', 'bid', 'ask', 'volume',
        'openInterest', 'impliedVolatility', 'inTheMoney'
    ]
    
    # 确保使用相同的列进行比较
    data1 = data1[columns_to_compare].sort_values(['strike']).reset_index(drop=True)
    data2 = data2[columns_to_compare].sort_values(['strike']).reset_index(drop=True)
    
    # 检查行数是否相同
    if len(data1) != len(data2):
        return False, ["期权链条数不一致"]
    
    # 对比每一列的值
    differences = []
    for col in columns_to_compare:
        if not data1[col].equals(data2[col]):
            differences.append(col)
    
    return len(differences) == 0, differences
'''
def check_market_status():
    """
    检查市场状态,判断是否需要更新数据
    返回布尔值(是否需要更新)和状态消息
    """
    current_time = get_current_time()
    
    # 检查是否是交易日
    if current_time.weekday() > 4:  # 周六和周日
        return False, "当前为非交易日,是否继续更新数据?"
    
    # 检查是否在交易时间内(美国东部时间 9:30 - 16:00)
    et_time = current_time.astimezone(pytz.timezone('US/Eastern'))
    market_open = et_time.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = et_time.replace(hour=23, minute=0, second=0, microsecond=0)
    
    # 如果不在交易时间内,返回提示信息
    if et_time < market_open or et_time > market_close:
        return False, "当前为非交易时间,是否继续更新数据?"
    
    return True, "市场开放中,可以更新数据"
'''
def precheck_data_update(db, target_date):
    """
    预检查是否需要更新数据
    返回布尔值(是否需要更新)和状态消息
    """
    check_symbols = ['AAPL', 'TSLA', 'NVDA']
    
    # 检查数据库是否为空
    for symbol in check_symbols:
        db_data = db.get_historical_chain(symbol, target_date)
        if db_data is None or db_data.empty:
            return True, "数据库中无数据,需要进行初始化更新"
    
    # 检查市场状态
    #should_update, status_msg = check_market_status()
    #if not should_update:
        #return False, status_msg
        
    # 其余检查逻辑保持不变
    new_data = {}
    old_data = {}
    
    try:
        for symbol in check_symbols:
            chain_data = fetch_options_data(symbol, target_date)
            if chain_data is None or chain_data.empty:
                return True, f"无法获取{symbol}的在线数据,需要更新"
            new_data[symbol] = chain_data
            time.sleep(1)
        
        for symbol in check_symbols:
            db_data = db.get_historical_chain(symbol, target_date)
            if db_data is None or db_data.empty:
                return True, f"本地数据库缺少{symbol}的数据,需要更新"
            old_data[symbol] = db_data
        
        for symbol in check_symbols:
            is_same, differences = compare_options_data(new_data[symbol], old_data[symbol])
            if not is_same:
                return True, f"{symbol}的数据有更新: {', '.join(differences)}"
        
        return False, "当前日期已是最新数据"
        
    except Exception as e:
        logging.error(f"预检查过程中发生错误: {str(e)}")
        return True, f"预检查过程发生错误,建议更新数据: {str(e)}"
    
def get_options_data(ticker, selected_date_str, db):
    """
    Get options data with optimized logic:
    1. Check YFinance connectivity status
    2. If connected, try online fetch first
    3. If offline or fetch fails, use local database
    """
    global YFINANCE_STATUS
    
    with YFINANCE_STATUS['lock']:
        is_connected = YFINANCE_STATUS['is_connected']
    
    if is_connected:
        try:
            # 首先尝试获取在线数据
            chain_data = fetch_options_data(ticker, selected_date_str)
            if chain_data is not None and not chain_data.empty:
                return chain_data, "online"
        except Exception as e:
            logging.error(f"Error fetching online data for {ticker}: {str(e)}")
    
    # 如果在线获取失败或系统离线，请尝试本地数据库
    historical_data = db.get_historical_chain(ticker, selected_date_str)
    if historical_data is not None and not historical_data.empty:
        return historical_data, "local"
    
    return None, None

class OptionsDatabase:
    def __init__(self, db_path="options_history.db"):
        self.db_path = db_path
        self.initialize_db()
    
    def initialize_db(self):
        """Create database and tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # 创建没有希腊字母的期权链表
        c.execute('''
            CREATE TABLE IF NOT EXISTS options_history (
                ticker TEXT,
                date TEXT,
                strike REAL,
                type TEXT,
                lastPrice REAL,
                bid REAL,
                ask REAL,
                volume INTEGER,
                openInterest INTEGER,
                impliedVolatility REAL,
                inTheMoney INTEGER,
                contractSymbol TEXT,
                timestamp TEXT,
                PRIMARY KEY (contractSymbol, timestamp)
            )
        ''')
        
        # 创建跟踪股票表
        c.execute('''
            CREATE TABLE IF NOT EXISTS tracked_tickers (
                ticker TEXT PRIMARY KEY,
                market TEXT,
                market_cap REAL,
                sector TEXT,
                last_updated TEXT,
                update_status TEXT
            )
        ''')
        # 创建更新历史记录表
        c.execute('''
            CREATE TABLE IF NOT EXISTS update_history (
                ticker TEXT,
                date TEXT,
                status TEXT,
                error_message TEXT,
                timestamp TEXT,
                PRIMARY KEY (ticker, date, timestamp)
            )
        ''')
        
        conn.commit()
        conn.close()

    def batch_save_options_chain(self, options_data_list):
        """Batch save multiple options chains to database"""
        if not options_data_list:
            return
            
        conn = sqlite3.connect(self.db_path)
        
        try:
            for chain_data, timestamp in options_data_list:
                if chain_data is not None and not chain_data.empty:
                    # 将 DataFrame 转换为 SQL 友好的格式
                    chain_data['timestamp'] = timestamp
                    
                    # 删除所有希腊字母列（如果存在）
                    columns_to_save = ['ticker', 'date', 'strike', 'type', 'lastPrice', 
                                     'bid', 'ask', 'volume', 'openInterest', 
                                     'impliedVolatility', 'inTheMoney', 'contractSymbol', 
                                     'timestamp']
                    
                    save_data = chain_data[columns_to_save].copy()
                    
                    # 插入新数据，如果存在则替换
                    save_data.to_sql('options_history', conn, if_exists='append', index=False)
            
            conn.commit()
            logging.info(f"Saved batch of {len(options_data_list)} options chains")
            self.update_last_update_time() 
        except Exception as e:
            logging.error(f"Error saving batch data: {str(e)}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_update_history(self, ticker, date, status, error_message=""):
        """Save update attempt history"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        timestamp = datetime.now().isoformat()
        
        c.execute('''
            INSERT INTO update_history (ticker, date, status, error_message, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (ticker, date, status, error_message, timestamp))
        
        conn.commit()
        conn.close()
    
    def update_ticker_status(self, ticker, status):
        """Update the status of a ticker in tracked_tickers"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        current_time = get_current_time().isoformat()
        
        c.execute('''
            UPDATE tracked_tickers 
            SET update_status = ?, last_updated = ?
            WHERE ticker = ?
        ''', (status, current_time, ticker))
        
        conn.commit()
        conn.close()

    def save_options_chain(self, ticker, chain_data, timestamp):
        """Save options chain data to database"""
        if chain_data is None or chain_data.empty:
            logging.warning(f"No data to save for {ticker}")
            return
            
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 将 DataFrame 转换为 SQL 友好的格式
            chain_data['timestamp'] = timestamp
            
            # 删除所有希腊字母列
            columns_to_save = ['ticker', 'date', 'strike', 'type', 'lastPrice', 
                             'bid', 'ask', 'volume', 'openInterest', 
                             'impliedVolatility', 'inTheMoney', 'contractSymbol', 
                             'timestamp']
            
            save_data = chain_data[columns_to_save].copy()
            
            # 插入新数据，如果存在则替换
            save_data.to_sql('options_history', conn, if_exists='append', index=False)
            
            conn.commit()
            logging.info(f"Saved options chain data for {ticker}")
            
        except Exception as e:
            logging.error(f"Error saving data for {ticker}: {str(e)}")
            conn.rollback()
        finally:
            conn.close()

    def get_historical_chain(self, ticker, date):
        """Retrieve historical options chain data with all necessary fields"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            WITH latest_timestamp AS (
                SELECT MAX(timestamp) as max_ts
                FROM options_history 
                WHERE ticker = ? AND date = ?
            )
            SELECT 
                ticker,
                date,
                strike,
                type,
                lastPrice,
                bid,
                ask,
                volume,
                openInterest,
                impliedVolatility,
                inTheMoney,
                contractSymbol,
                timestamp
            FROM options_history 
            WHERE ticker = ? 
            AND date = ?
            AND timestamp = (SELECT max_ts FROM latest_timestamp)
        '''
        
        try:
            df = pd.read_sql_query(
                query, 
                conn, 
                params=(ticker, date, ticker, date)
            )
            
            # 确保数值类型正确
            numeric_columns = [
                'strike', 'lastPrice', 'bid', 'ask', 
                'volume', 'openInterest', 'impliedVolatility'
            ]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 确保布尔值类型正确
            df['inTheMoney'] = df['inTheMoney'].astype(bool)
            
            return df
        except Exception as e:
            logging.error(f"Error retrieving historical chain: {str(e)}")
            return None
        finally:
            conn.close()

    def update_tracked_tickers(self, tickers_data):
        """Update the list of tracked tickers"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        current_time = get_current_time().isoformat()
        
        for ticker, market, market_cap in tickers_data:
            c.execute('''
                INSERT OR REPLACE INTO tracked_tickers (ticker, market, market_cap, last_updated)
                VALUES (?, ?, ?, ?)
            ''', (ticker, market, market_cap, current_time))
        
        conn.commit()
        conn.close()

    def get_tracked_tickers(self):
        """Get list of all tracked tickers"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('SELECT * FROM tracked_tickers', conn)
        conn.close()
        return df

    def update_last_update_time(self):
        """Update the last_updated timestamp for all tickers"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        current_time = get_current_time().isoformat()
        
        c.execute('''
            UPDATE tracked_tickers 
            SET last_updated = ?
        ''', (current_time,))
        
        conn.commit()
        conn.close()


def get_market_tickers():
    """Get tickers from multiple markets with market cap info"""
    try:
        tickers_data = []
        
        # 使用 yfinance 获取市值和行业的功能
        def get_ticker_info(ticker):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                time.sleep(random.uniform(0.5, 1))
                return {
                    'marketCap': info.get('marketCap', 0),
                    'sector': info.get('sector', 'Unknown')
                }
            except Exception as e:
                logging.error(f"Error getting info for {ticker}: {str(e)}")
                return {'marketCap': 0, 'sector': 'Unknown'}

        # 获取标普500信息
        try:
            sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
            logging.info(f"Retrieved {len(sp500)} S&P 500 companies")
            for _, row in sp500.iterrows():
                info = get_ticker_info(row['Symbol'])
                tickers_data.append([  # 修改这里：使用列表而不是元组
                    row['Symbol'], 
                    'S&P500', 
                    info['marketCap']
                ])
        except Exception as e:
            logging.error(f"Error getting S&P 500 data: {str(e)}")

        return tickers_data

    except Exception as e:
        logging.error(f"Error in get_market_tickers: {str(e)}")
        return []
    
def process_ticker(ticker, db, target_date):
    """Process a single ticker with improved error handling and rate limiting"""
    try:
        if should_update_database(target_date):
            # 将状态更新为“处理中(processing)”
            db.update_ticker_status(ticker, "processing")
            
            # 2 到 4 秒之间的随机延迟以避免速率限制
            time.sleep(random.uniform(2, 4))
            
            chain_data = fetch_options_data(ticker, target_date)
            if chain_data is not None:
                db.save_options_chain(ticker, chain_data, datetime.now().isoformat())
                db.save_update_history(ticker, target_date, "success")
                db.update_ticker_status(ticker, "completed")
                db.update_last_update_time() 
            else:
                db.save_update_history(ticker, target_date, "failed", "No data available")
                db.update_ticker_status(ticker, "failed")
    
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Error processing {ticker}: {error_msg}")
        db.save_update_history(ticker, target_date, "error", error_msg)
        db.update_ticker_status(ticker, "error")

def get_next_friday():
    """Get the next Friday's date"""
    today = datetime.now(pytz.timezone('Asia/Shanghai'))
    days_ahead = 4 - today.weekday()  # 星期五 4点
    if days_ahead <= 0:
        days_ahead += 7
    if today.hour >= 4:  # 北京时间4点后
        if days_ahead == 7:
            days_ahead += 7
    return today + timedelta(days=days_ahead)

def should_update_database(target_date):
    """Check if database should be updated for target date"""
    current_date = datetime.now(pytz.timezone('Asia/Shanghai')).date()
    target_date = pd.to_datetime(target_date).date()
    
    # 如果目标日期已过，则不要更新
    if target_date < current_date:
        return False
    
    # 如果目标日期是今天或将来，请更新
    return True

def fetch_options_data(ticker, expiry_date):
    """Fetch options chain data from yfinance with improved error handling and logging"""
    try:
        stock = yf.Ticker(ticker)
        chain = stock.option_chain(expiry_date)
        
        if chain is None:
            logging.warning(f"No options data available for {ticker}")
            return None
            
        # 处理调用
        calls = chain.calls
        calls['type'] = 'CALL'
        
        # 流程投入
        puts = chain.puts
        puts['type'] = 'PUT'
        
        # 合并并添加元数据
        combined = pd.concat([calls, puts])
        combined['ticker'] = ticker
        combined['date'] = expiry_date
        
        logging.info(f"Successfully fetched options data for {ticker}")
        return combined
        
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {str(e)}")
        return None

def calculate_delta(S, K, sigma, option_type):
    """Simplified delta calculation"""
    # 简单的计算,使用 Black-Scholes 公式
    moneyness = S / K
    if option_type == 'CALL':
        return max(min(moneyness - 0.5, 1), 0)
    else:
        return max(min(0.5 - moneyness, 0), -1)

def calculate_gamma(S, K, sigma):
    """Simplified gamma calculation"""
    # 简单的计算,使用 Black-Scholes 公式
    return max(0, 1 - abs(S/K - 1))

def update_database():
    """Update database with latest options data for all tracked tickers"""
    st.write("Starting database update process...")
    
    # 为初始状态创建占位符
    init_status = st.empty()
    
    # 初始化数据库
    db = OptionsDatabase()
    
    try:
        # 获取下个星期五期权链信息
        next_friday = get_next_friday()
        next_friday_str = next_friday.strftime('%Y-%m-%d')
        
        # 执行预检查
        should_update, status_msg = precheck_data_update(db, next_friday_str)
        
        if not should_update:
            st.info(status_msg)
            return
           
        st.info(f"需要更新数据: {status_msg}")
            
        # 首先检查数据库中是否有最新数据
        db_tickers = db.get_tracked_tickers()
        
        if not db_tickers.empty:
            # 将 last_updated 转换为对应时区日期时间
            last_update = pd.to_datetime(db_tickers['last_updated'].max())
            if not last_update.tzinfo:
                last_update = pytz.timezone('Asia/Shanghai').localize(last_update)
            
            current_time = get_current_time()
            
            # 现在两个时间都可以识别时区以进行比较
            if (current_time - last_update).days <= 180: 
                st.success(f"Using stored tickers from last update on {last_update.strftime('%Y-%m-%d')}")
                total_tickers = len(db_tickers)
                
                # 显示数据库中的股票行情明细
                col1, col2, col3 = st.columns(3)
                sp500_count = len(db_tickers[db_tickers['market'] == 'S&P500'])
                col1.metric("S&P 500", sp500_count)
                
                # 将数据库记录转换为预期格式
                tickers_data = [
                    (row['ticker'], row['market'], row['market_cap']) 
                    for _, row in db_tickers.iterrows()
                ]
            else:
                # 数据超过 6 个月，需要获取新数据
                init_status.info("Fetching market tickers (data older than 6 months)...")
                with st.spinner("Retrieving market data..."):
                    tickers_data = get_market_tickers()
                
                if not tickers_data:
                    st.error("Failed to retrieve market tickers")
                    return
                
                total_tickers = len(tickers_data)
                st.success(f"Successfully retrieved {total_tickers} tickers")
                
                # 显示新代码的细分
                col1, col2, col3 = st.columns(3)
                sp500_count = len([t for t in tickers_data if t[1] == 'S&P500'])
                col1.metric("S&P 500", sp500_count)
                
                # 使用新的代码更新数据库
                st.info("Updating tracked tickers in the database...")
                db.update_tracked_tickers(tickers_data)
        else:
            # 数据库中没有数据，需要获取所有内容
            init_status.info("Fetching market tickers (first time initialization)...")
            with st.spinner("Retrieving market data..."):
                tickers_data = get_market_tickers()
            
            if not tickers_data:
                st.error("Failed to retrieve market tickers")
                return
            
            total_tickers = len(tickers_data)
            st.success(f"Successfully retrieved {total_tickers} tickers")
            
            # 显示新代码的细分
            col1, col2, col3 = st.columns(3)
            sp500_count = len([t for t in tickers_data if t[1] == 'S&P500'])
            col1.metric("S&P 500", sp500_count)
            
            # 使用新的代码初始化数据库
            st.info("Initializing database with tracked tickers...")
            db.update_tracked_tickers(tickers_data)
            
        # 创建用于进度跟踪的容器
        progress_container = st.container()
        with progress_container:
            st.write("Processing tickers...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 创建两列用于状态显示
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Recent Updates")
                recent_updates = st.empty()
            
            with col2:
                st.write("Statistics")
                stats_container = st.empty()
            
            # 初始化计数器
            stats = {
                'completed': 0,
                'failed': 0,
                'error': 0,
                'total': total_tickers
            }
            
            # 初始化最近更新列表
            recent = []
            max_recent = 5  # 显示的最近更新数量
            
            def update_display():
                # 更新进度条
                progress = (stats['completed'] + stats['failed'] + stats['error']) / stats['total']
                progress_bar.progress(progress)
                
                # 更新状态文本
                status_text.text(f"Processed {stats['completed'] + stats['failed'] + stats['error']}/{stats['total']} tickers")
                
                # 更新统计数据
                stats_df = pd.DataFrame({
                    'Status': ['Completed', 'Failed', 'Error', 'Remaining'],
                    'Count': [
                        stats['completed'],
                        stats['failed'],
                        stats['error'],
                        stats['total'] - (stats['completed'] + stats['failed'] + stats['error'])
                    ]
                })
                stats_container.dataframe(stats_df, use_container_width=True)
                
                # 更新近期活动
                if recent:
                    recent_df = pd.DataFrame(recent, columns=['Time', 'Ticker', 'Status'])
                    recent_updates.dataframe(recent_df, use_container_width=True)
            
            options_data_batch = []
            batch_size = 10
            
            # 在有限的工作线程中使用 
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_ticker = {
                    executor.submit(fetch_options_data, ticker, next_friday_str): ticker 
                    for ticker, _, _ in tickers_data
                }
                
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    current_time = datetime.now().strftime('%H:%M:%S')
                    
                    try:
                        chain_data = future.result()
                        timestamp = datetime.now().isoformat()
                        
                        if chain_data is not None and not chain_data.empty:
                            options_data_batch.append((chain_data, timestamp))
                            status = "Completed"
                            stats['completed'] += 1
                        else:
                            status = "Failed"
                            stats['failed'] += 1
                        
                    except Exception as e:
                        logging.error(f"Error processing {ticker}: {str(e)}")
                        status = "Error"
                        stats['error'] += 1
                    
                    # 更新最近列表
                    recent.insert(0, [current_time, ticker, status])
                    recent = recent[:max_recent]  
                    
                    # 更新显示
                    update_display()
                    
                    # 如果达到批次大小，则保存批次
                    if len(options_data_batch) >= batch_size:
                        db.batch_save_options_chain(options_data_batch)
                        options_data_batch = []
                    
                    # 在请求之间添加延迟
                    time.sleep(random.uniform(1, 2))
            
            # 保存所有剩余的选项数据
            if options_data_batch:
                db.batch_save_options_chain(options_data_batch)
            
            # 最终状态更新
            progress_bar.progress(1.0)
            st.success(f"""
                Update completed:
                - Successfully processed: {stats['completed']}
                - Failed: {stats['failed']}
                - Errors: {stats['error']}
                - Total processed: {stats['total']}
            """)

    except Exception as e:
        st.error(f"Error during market data retrieval: {str(e)}")
        logging.error(f"Market data retrieval error: {str(e)}")
        return

def calculate_summary_metrics(chain_data):
    """Calculate summary metrics for the options chain"""
    if chain_data is None or chain_data.empty:
        return None
        
    metrics = {
        'Total Volume': chain_data['volume'].sum(),
        'Total Open Interest': chain_data['openInterest'].sum(),
        'Put/Call Ratio': (
            chain_data[chain_data['type'] == 'PUT']['volume'].sum() /
            chain_data[chain_data['type'] == 'CALL']['volume'].sum()
        ),
        'Average IV': chain_data['impliedVolatility'].mean() * 100,
        'Max Strike': chain_data['strike'].max(),
        'Min Strike': chain_data['strike'].min(),
        'Most Active Strike': chain_data.groupby('strike')['volume'].sum().idxmax()
    }
    
    return metrics

def create_volume_distribution_chart(chain_data):
    """Create volume distribution chart"""
    if chain_data is None or chain_data.empty:
        return None
        
    # 准备数据以进行可视化
    calls = chain_data[chain_data['type'] == 'CALL'].copy()
    puts = chain_data[chain_data['type'] == 'PUT'].copy()
    
    fig = go.Figure()
    
    # 添加看涨期权交易量
    fig.add_trace(go.Bar(
        x=calls['strike'],
        y=calls['volume'],
        name='Calls Volume',
        marker_color='green'
    ))
    
    # 添加看跌期权交易量
    fig.add_trace(go.Bar(
        x=puts['strike'],
        y=-puts['volume'], 
        name='Puts Volume',
        marker_color='red'
    ))
    
    fig.update_layout(
        title='Volume Distribution by Strike Price',
        xaxis_title='Strike Price',
        yaxis_title='Volume',
        barmode='relative'
    )
    
    return fig

def render_options_chain(chain_data):
    """Render options chain with enhanced features"""
    if chain_data is None or chain_data.empty:
        st.error("No data available")
        return
    
    # 确保所有必需的列都存在
    required_columns = [
        'strike', 'lastPrice', 'bid', 'ask', 'volume',
        'openInterest', 'impliedVolatility', 'inTheMoney', 'type'
    ]
    
    missing_columns = [col for col in required_columns if col not in chain_data.columns]
    if missing_columns:
        st.error(f"Missing required columns: {missing_columns}")
        return
    
    # 计算并显示汇总指标
    metrics = calculate_summary_metrics(chain_data)
    if metrics:
        st.subheader("Summary Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Volume", f"{int(metrics['Total Volume']):,}")
            st.metric("Put/Call Ratio", f"{metrics['Put/Call Ratio']:.2f}")
            
        with col2:
            st.metric("Total Open Interest", f"{int(metrics['Total Open Interest']):,}")
            st.metric("Average IV", f"{metrics['Average IV']:.1f}%")
            
        with col3:
            st.metric("Most Active Strike", f"${float(metrics['Most Active Strike']):.2f}")
    
    # 分离看涨和看跌期权
    calls = chain_data[chain_data['type'] == 'CALL'].copy()
    puts = chain_data[chain_data['type'] == 'PUT'].copy()
    
    # 创建两列显示
    col1, col2 = st.columns(2)
    
    # 定义要显示的列
    display_columns = [
        'strike', 'lastPrice', 'bid', 'ask', 'volume',
        'openInterest', 'impliedVolatility'
    ]
    
    # 格式化数字列
    numeric_format = {
        'lastPrice': lambda x: f"${float(x):.2f}" if pd.notnull(x) else '',
        'bid': lambda x: f"${float(x):.2f}" if pd.notnull(x) else '',
        'ask': lambda x: f"${float(x):.2f}" if pd.notnull(x) else '',
        'strike': lambda x: f"${float(x):.2f}" if pd.notnull(x) else '',
        'impliedVolatility': lambda x: f"{float(x):.1%}" if pd.notnull(x) else '',
        'volume': lambda x: f"{int(x):,}" if pd.notnull(x) else '0',
        'openInterest': lambda x: f"{int(x):,}" if pd.notnull(x) else '0'
    }
    
    # 显示看涨期权
    with col1:
        st.subheader("Calls")
        calls_display = calls[display_columns].copy()
        for col in calls_display.columns:
            if col in numeric_format:
                calls_display[col] = calls_display[col].apply(numeric_format[col])
        st.dataframe(calls_display, use_container_width=True)
    
    # 显示看跌期权
    with col2:
        st.subheader("Puts")
        puts_display = puts[display_columns].copy()
        for col in puts_display.columns:
            if col in numeric_format:
                puts_display[col] = puts_display[col].apply(numeric_format[col])
        st.dataframe(puts_display, use_container_width=True)
    
    # 显示成交量分布图表
    st.subheader("Volume Distribution")
    fig = create_volume_distribution_chart(chain_data)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

def main():
    st.set_page_config(layout="wide")
    st.title("Enhanced Options Chain Viewer")
    
    # 应用程序启动时启动连接检查器
    if 'connectivity_checker_started' not in st.session_state:
        start_connectivity_checker()
        st.session_state.connectivity_checker_started = True
    
    # 初始化数据库
    db = OptionsDatabase()
    
    # 获取跟踪代码并按市值排序
    tracked_tickers = db.get_tracked_tickers()
    tracked_tickers = tracked_tickers.sort_values('market_cap', ascending=False)

    # 用户输入
    col1, col2, col3 = st.columns(3)
    
    with col1:
        default_ticker = "AAPL"
        ticker_list = tracked_tickers['ticker'].tolist() if not tracked_tickers.empty else [default_ticker]
        ticker = st.selectbox("Enter Stock Ticker:", ticker_list, key="ticker_selectbox").upper()
    
    with col2:
        selected_date = st.date_input(
            "Select Date:",
            value=get_next_friday()
        )
    
    with col3:
        # 显示 YFinance 连接状态
        with YFINANCE_STATUS['lock']:
            is_connected = YFINANCE_STATUS['is_connected']
            last_check = YFINANCE_STATUS['last_check']
        
        status_color = "green" if is_connected else "red"
        status_text = "Online" if is_connected else "Offline"
        st.markdown(f"""
            <div style='padding: 10px; border-radius: 5px; background-color: {status_color}15;'>
                <strong>YFinance Status:</strong> 
                <span style='color: {status_color};'>{status_text}</span>
                <br>
                <small>Last check: {last_check.strftime('%H:%M:%S')}</small>
            </div>
        """, unsafe_allow_html=True)
        
        if st.button("Update Database", key="update_db"):
            try:
                with st.spinner("Updating database with latest options data..."):
                    update_database()
                st.success("Database update completed!")
            except Exception as e:
                st.error(f"Error updating database: {str(e)}")
                logging.error(f"Database update error: {str(e)}")
    
    # Query按钮处理
    if st.button("Query Options Chain", key="query_chain", use_container_width=True):
        selected_date_str = selected_date.strftime('%Y-%m-%d')
        current_date = datetime.now().date()
        
        with st.spinner("Fetching options data..."):
            try:
                # 检查yfinance连接状态
                with YFINANCE_STATUS['lock']:
                    is_connected = YFINANCE_STATUS['is_connected']
                
                # 检查日期是否是历史日期
                is_historical = selected_date <= current_date
                
                # 优先尝试在线获取数据，除非是历史数据或网络断开
                if is_connected and not is_historical:
                    # 尝试在线获取数据
                    chain_data = fetch_options_data(ticker, selected_date_str)
                    if chain_data is not None and not chain_data.empty:
                        st.success(f"Retrieved latest options data for {ticker}")
                        render_options_chain(chain_data)
                        # 保存到数据库以供将来使用
                        db.save_options_chain(ticker, chain_data, datetime.now().isoformat())
                        return
                
                # 如果在线获取失败或是历史数据，尝试从数据库获取
                historical_data = db.get_historical_chain(ticker, selected_date_str)
                if historical_data is not None and not historical_data.empty:
                    if is_historical:
                        st.info(f"Using historical data from database for {ticker}")
                    else:
                        st.warning("Online data fetch failed. Using latest available data from database.")
                    render_options_chain(historical_data)
                else:
                    st.error(f"No options data available for {ticker} on {selected_date_str}")
                        
            except Exception as e:
                st.error(f"Error fetching data: {str(e)}")
                logging.error(f"Error fetching data for {ticker}: {str(e)}")


    # 显示数据库上次更新时间
    if not tracked_tickers.empty:
        last_update = pd.to_datetime(tracked_tickers['last_updated'].max())
        if not last_update.tzinfo:
            last_update = pytz.timezone('Asia/Shanghai').localize(last_update)
            
        st.sidebar.subheader("Last Database Update")
        st.sidebar.info(last_update.strftime('%Y-%m-%d %H:%M:%S'))
        
        # 添加下次预定更新时间
        next_update = get_current_time().replace(
            hour=4, minute=0, second=0, microsecond=0
        )
        if next_update <= get_current_time():
            next_update += timedelta(days=1)
        st.sidebar.subheader("Next Scheduled Update")
        st.sidebar.info(next_update.strftime('%Y-%m-%d %H:%M:%S'))
    
    st.markdown("""
        <div style='text-align: center; color: grey; font-size: 0.8em;'>
            Data updates automatically at 04:00 AM Beijing Time daily. 
            Historical data is stored locally, while real-time data is fetched from yfinance.
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
