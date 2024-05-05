import pandas as pd
import adata
import json
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import os
import datetime

def get_nth_high_info(df, n):
    """
    提取DataFrame中第n高的价格及其日期和索引。
    
    :param df: 包含股票数据的DataFrame,至少需要包含'high'和'trade_date'列。
    :param n: 需要获取的高值的排名。
    :return: 包含日期、值和索引的元组。如果n大于df中的数据点数量,则返回None。
    """
    # 检查是否有足够的数据点
    if df is None or len(df) < n:
        return None, None, None

    try:
        # 获取n个最高价格的索引
        top_n_highs = df['high'].nlargest(n)
        # 由于nlargest可能返回多于n个结果（如果有并列的情况）,我们需要获取正确的最后一个
        high_index = top_n_highs.index[n-1]
        high_value = top_n_highs.iloc[n-1]
        high_date = pd.to_datetime(df.at[high_index, 'trade_date']).date()
        return high_date, high_value, high_index
    except Exception as e:
        print(f"获取第{n}个高值信息时出错：{e}")
        return None, None, None

# 修改后的重试装饰器
def retry(exception_to_check, tries=4, delay=3, backoff=2, error_log='error_log.txt'):
    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            local_tries, local_delay = tries, delay
            while local_tries > 1:
                try:
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    print(f"{func.__name__} 函数运行出错,尝试重新运行。错误: {e}")
                    time.sleep(local_delay)
                    local_tries -= 1
                    local_delay *= backoff
            try:
                return func(*args, **kwargs)
            except exception_to_check as e:
                with open(error_log, 'a') as f:
                    f.write(f"在处理函数 {func.__name__} 时遇到错误: {str(e)},参数：{args}, {kwargs}\n")
                return None
        return wrapper
    return decorator_retry

# 筛选逻辑一：10天价格是否持续上升
def is_price_increasing(df):
    if df is None or len(df) < 10:
        return False
    last_10_days_highs = df['high'].tail(10)
    return all(last_10_days_highs.iloc[i] < last_10_days_highs.iloc[i + 1] for i in range(len(last_10_days_highs) - 1))

# 筛选逻辑二：过去30天到7天内高点最高
def is_30_to_7_high_max(df):
    if df is None or len(df) < 30:
        return False
    high_last_30_to_7 = df["high"].iloc[-30:-7]
    last_seven_highs = df['high'].iloc[-7:]
    return high_last_30_to_7.max() <= last_seven_highs.max()

# 筛选逻辑三：过去30天到23天内高点<7 天高点
def filter_logic_three(df):
    """
    筛选逻辑三：基于高点的位置和价格进行筛选。
    """
    if df is None or df.empty:
        return False

    df['high'] = df['high'].astype(float)
    last_index = df.index[-1]
    high_last_30_to_7 = df["high"].iloc[last_index-7:last_index-30:-1]
    last_seven_highs = df['high'].iloc[-7:]
    top_two_highs = last_seven_highs.nlargest(2)
    
    if len(top_two_highs) < 2:
        return False

    first_high_date, first_high_value, first_high_index = get_nth_high_info(df, 1)
    second_high_date, second_high_value, second_high_index = get_nth_high_info(df, 2)
    third_high_date, third_high_value, third_high_index = get_nth_high_info(df, 3)

    first_second = abs(first_high_index - second_high_index) != 1
    first_third = abs(first_high_index - third_high_index) != 1
    is_first_high_date_last_date = df['trade_date'].iloc[first_high_index] == df['trade_date'].iloc[-1]
    is_3023_high_7_high = high_last_30_to_7.max() <= last_seven_highs.max()

    return (is_first_high_date_last_date and (first_second or first_third))

# 筛选逻辑四：过去30天最高价等于 7 天最高价
def is_seven_equals_thirty_high(df):
    """
    检查最近7天的最高价是否等于过去30天的最高价。
    :param df: 包含股票数据的DataFrame,至少需要包含'high'列。
    :return: 如果7天内的最高价等于30天内的最高价,返回True,否则返回False。
    """
    if df is None or len(df) < 30:
        return False

    # 计算最近7天和30天的最高价格
    max_seven_day_high = df['high'].tail(7).max()
    max_thirty_day_high = df['high'].tail(30).max()

    # 检查是否相等
    return max_seven_day_high == max_thirty_day_high

@retry(Exception, tries=3, delay=2, backoff=2)
def process_stock(stock_code, filters=[]):
    """
    Processes stock data using a list of filtering functions.
    """
    try:
        data = adata.stock.market.get_market(stock_code=stock_code, start_date='2024-03-01', k_type=1)
        df = pd.DataFrame(data)
        if df.empty:
            return None

        # Apply all filtering logics
        for filter_func in filters:
            if not filter_func(df):
                return None  # If any filter fails, skip this stock

        # Return some useful stock information
        return {
            "stock_code": stock_code,
            "short_name": str(stocks_info[stocks_info['stock_code'] == stock_code]['short_name'].values[0]),
            "exchange": str(stocks_info[stocks_info['stock_code'] == stock_code]['exchange'].values[0]),
            "list_date": str(stocks_info[stocks_info['stock_code'] == stock_code]['list_date'].values[0]),
        }
    except Exception as e:
        print(f"处理股票 {stock_code} 时出现意外错误：{e}")
        raise

def get_stocks_info():
    # 获取股票信息
    stocks_info = adata.stock.info.all_code()
    # 如果股票代码已是字符串但要保证六位数格式，需要适当地填充前导零
    stocks_info['stock_code'] = stocks_info['stock_code'].apply(lambda x: x.zfill(6))

    # 将日期中的NaT或None转换为'NaN'，并确保整个列为字符串格式
    stocks_info['list_date'] = stocks_info['list_date'].fillna('NaN').astype(str)

   # 获取当前日期并格式化为所需的字符串形式
    today_date = datetime.date.today().strftime('%Y-%m-%d')

    # 构建文件名
    filename = f'stock_info_{today_date}.json'

    # 检查文件是否存在
    if not os.path.exists(filename):
        # 如果文件不存在，则将数据存储到JSON文件中
        stocks_info.to_json(filename, orient='records', force_ascii=False, index=False)
        print(f"文件 '{filename}' 不存在，已创建并保存数据。")
    else:
        print(f"文件 '{filename}' 已存在，不需要保存数据。")

def create_html():
    # 定义HTML内容
    html_content = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>K线图与股票筛选</title>
        <style>
            body, html {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            #container {
                display: flex;
                height: 100vh;
            }
            #left-container {
                flex: 1;
                overflow: auto;
            }
            #kline-chart-container {
                flex: 2;
                display: flex;
                flex-direction: column;
            }
            #kline-chart {
                flex-grow: 1;
            }
            table {
                width: 100%;
                border-collapse: collapse;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            .period-button {
                background-color: blue;
                color: white;
                border: none;
                padding: 5px 10px;
                margin: 2px;
                cursor: pointer;
            }
            .period-button:hover {
                background-color: darkblue;
            }
            .selected {
                background-color: rgb(218, 0, 0) !important;
                color: white !important;
            }
        </style>
    </head>
    <body>
    <div id="container">
        <div id="left-container">
            <div id="top-section">
                <div id="filtered-stock-count">已筛选出 0 支股票</div>
                <div id="checkbox-container">
                    <input type="checkbox" id="sz-checkbox" value="SZ" checked> SZ
                    <input type="checkbox" id="sh-checkbox" value="SH" checked> SH
                    <input type="checkbox" id="bj-checkbox" value="BJ"> BJ
                </div>
            </div>
            <div id="scrollable-table-container">
                <table id="stock-table">
                    <thead>
                        <tr>
                            <th>Stock Code</th>
                            <th>Short Name</th>
                            <th>Exchange</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- 表格内容将在这里动态插入 -->
                    </tbody>
                </table>
            </div>
        </div>
        <div id="kline-chart-container">
            <div id="period-buttons">
                <button class="period-button" data-period="1">日K</button>
                <button class="period-button" data-period="7">周K</button>
                <button class="period-button" data-period="30">季K</button>
                <button class="period-button" data-period="180">半年K</button>
                <button class="period-button" data-period="365">年K</button>
            </div>
            <div id="kline-chart">
                <!-- TradingView K线图将在这里显示 -->
            </div>
        </div>
    </div>
    <script src="https://s3.tradingview.com/tv.js"></script>
    <script>
        let currentStock = null; // 假设这是一个全局变量来存储当前选中的股票数据

        document.addEventListener("DOMContentLoaded", function() {
            const filteredStockCountElement = document.getElementById('filtered-stock-count');
            const szCheckbox = document.getElementById('sz-checkbox');
            const shCheckbox = document.getElementById('sh-checkbox');
            const bjCheckbox = document.getElementById('bj-checkbox');
            const tbody = document.getElementById('stock-table').getElementsByTagName('tbody')[0];
            const periodButtons = document.querySelectorAll('.period-button');
            periodButtons.forEach(button => {
                button.addEventListener('click', function() {
                    if (currentStock) {
                        updateKLineChart(currentStock, button.dataset.period);
                        highlightButton(button);
                    }
                });
            });

            function highlightButton(selectedButton) {
                periodButtons.forEach(button => {
                    button.classList.remove('selected');
                });
                selectedButton.classList.add('selected');
            }

            highlightButton(periodButtons[0]); // 默认选中日K

            szCheckbox.addEventListener('change', fetchStockData);
            shCheckbox.addEventListener('change', fetchStockData);
            bjCheckbox.addEventListener('change', fetchStockData);

            fetchStockData();

            function fetchStockData() {
                tbody.innerHTML = '';
                let filteredStockCount = 0;
                // 获取今天的日期
                var today = new Date();
                var year = today.getFullYear();
                var month = ('0' + (today.getMonth() + 1)).slice(-2); // 月份从0开始，需要加1，并且保证两位数格式
                var day = ('0' + today.getDate()).slice(-2); // 日期需要保证两位数格式

                // 构建文件名
                var fileName = 'result_' + year + '-' + month + '-' + day + '.json';

                // 发起请求
                fetch(fileName)
                .then(response => response.json())
                .then(data => {
                    data.forEach(stock => {
                        if ((szCheckbox.checked && stock.exchange === 'SZ') ||
                            (shCheckbox.checked && stock.exchange === 'SH') ||
                            (bjCheckbox.checked && stock.exchange === 'BJ')) {
                            const row = tbody.insertRow();
                            const cell1 = row.insertCell(0);
                            cell1.textContent = stock.stock_code;

                            const cell2 = row.insertCell(1);
                            cell2.textContent = stock.short_name;

                            const cell3 = row.insertCell(2);
                            cell3.textContent = stock.exchange;

                            row.addEventListener("click", function() {
                                currentStock = stock; // 更新当前选中的股票
                                updateKLineChart(stock, document.querySelector('.selected').dataset.period);
                                highlightRow(row);
                            });

                            filteredStockCount++;
                        }
                    });

                    filteredStockCountElement.textContent = `已筛选出 ${filteredStockCount} 支股票`;
                })
                .catch(error => console.error('Error loading the data:', error));
            }

            function updateKLineChart(stock, period) {
                const exchangePrefix = {
                    'SZ': 'SZSE:',
                    'SH': 'SSE:',
                    'BJ': 'BSE:'
                };

                new TradingView.widget({
                    "container_id": "kline-chart",
                    "symbol": exchangePrefix[stock.exchange] + stock.stock_code,
                    "interval": period,
                    "timezone": "Etc/UTC",
                    "theme": "light",
                    "style": "1",
                    "locale": "zh_CN",
                    "toolbar_bg": "#f1f3f6",
                    "enable_publishing": false,
                    "hide_side_toolbar": false,
                    "allow_symbol_change": true,
                    "width": "100%",
                    "height": "100%",
                    "overrides": {
                        "mainSeriesProperties.candleStyle.upColor": "#DB0000", // 上涨的蜡烛颜色 - 亮红色
                        "mainSeriesProperties.candleStyle.downColor": "#008000", // 下跌的蜡烛颜色 - 办公绿
                        "mainSeriesProperties.candleStyle.borderUpColor": "#DB0000", // 上涨蜡烛的边框颜色 - 亮红色
                        "mainSeriesProperties.candleStyle.borderDownColor": "#008000", // 下跌蜡烛的边框颜色 - 办公绿
                        "mainSeriesProperties.candleStyle.wickUpColor": "#DB0000", // 上涨蜡烛的影线颜色 - 亮红色
                        "mainSeriesProperties.candleStyle.wickDownColor": "#008000" // 下跌蜡烛的影线颜色 - 办公绿
                    }            });
            }

            function highlightRow(row) {
                Array.from(tbody.children).forEach(r => {
                    r.classList.remove("selected");
                });
                row.classList.add("selected");
            }
        });
    </script>
    </body>
    </html>
    '''

    # 写入文件
    with open('index.html', 'w', encoding='utf-8') as file:
        file.write(html_content)

    print("HTML file 'index.html' has been created.")

# 获取当前日期并格式化为所需的字符串形式
today_date = datetime.date.today().strftime('%Y-%m-%d')
# 拼接文件名
file_name = f"result_{today_date}.json"

# 检查文件是否存在
if not os.path.exists(file_name):
    get_stocks_info()
    # 构建文件名
    filename = f'stock_info_{today_date}.json'
    stocks_info = pd.read_json(filename, dtype={'stock_code': str})
    stock_codes = stocks_info['stock_code']

    results = []
    num_cores = os.cpu_count()
    pool_size = max(10, num_cores * 2)

    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        # 指定在函数调用中应用哪些筛选器
        #选择逻辑
        filters_to_apply = [filter_logic_three]
        futures = {executor.submit(process_stock, stock_code, filters=filters_to_apply): stock_code for stock_code in stock_codes}
        progress = tqdm(as_completed(futures), total=len(stock_codes), desc="处理股票")

        for future in progress:
            result = future.result()
            if result:
                results.append(result)
            progress.set_description("处理中: {}".format(futures[future]))
            
    # 将结果写入JSON文件
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    print(f"筛选结果已保存至'{file_name}'。")
else:
    print(f"今天的数据已经存在于文件'{file_name}'中。")

# 打开 html
if os.path.exists('index.html'):
    os.system("open index.html")
else:
    create_html()
    os.system("open index.html")


