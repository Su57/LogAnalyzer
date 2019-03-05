import os
import re
import json
from datetime import datetime

LOG_REGULAR_EXPRESSIONS = {

    # TODO 完善其他项目log的正则匹配

    'fukuoka': {

        'sp': {
            'index_pattern': ['/'],
            'pattern': '(?P<remote_addr>[\d\.]{7,})\s-\s-\s(?:\[(?P<datetime>[^\[\]]+)\])\s"(?P<request>[^"]+)"\s(?P<status>\d+)\s(?P<size>\d+)\s"(?:[^"]+)"\s"(?P<user_agent>[^"]+)"',
            'route_patterns': ['/schedule/m_search', '/schedule/m_search_detail', '/mobilet'],
            'diagram_patterns': ['/schedule/m_eki_diagram_k', '/schedule/m_eki_diagram_n'],
            'fare_patterns': ['/fare/fare_index']
        },

        'pc': {
            'index_pattern': ['/'],
            'pattern': '(?P<remote_addr>[\d\.]{7,})\s(?P<identity>[\w.-]+)\s(?P<user_id>[\w.-]|"+)\s(?:\[(?P<datetime>[^\[\]]+)\])\s"(?P<request>[^"]+)"\s(?P<status>\d+)\s(?P<size>\d+|-)\s(?P<response_time>\d+)\s"(?:[^"]+)"\s"(?P<user_agent>[^"]+)"',
            'route_pattern': ['route', 'nsresult'],
            'diagram_pattern': ['diagram'],
            'fare_pattern': ['fare'],
        }
    },

}

JAPANESE_WEEKDAY_NAMES = {
    0: '日曜日',
    1: '月曜日',
    2: '火曜日',
    3: '水曜日',
    4: '木曜日',
    5: '金曜日',
    6: '土曜日',
}

ALLOWED_SERVER_NAMES = ['fukuoka', 'zentanbus']

ALLOWED_SERVER_TYPES = ['pc', 'sp']  # 传统机暂时使用sp来表示


class LogAnalyzer:

    """
    使用参数说明：
        month: 要进行统计的月份，默认为当前月份-1，int类型
        server_name: 必须参数 -- 项目名称，默认为fukuoka。合法项目名在ALLOWED_SERVER_NAMES列表中
        server_type: 必须参数 -- 服务器类型，sp/pc  ==> 传统机/pc机
        output_dir: 程序输出的文件路径，包括结果json和异常日志,默认是和程序在同层目录
        collect_invalid: 是否输出非有效的记录。默认False
    """

    def __init__(self, logfile_path, month=datetime.today().month - 1, server_name='fukuoka', server_type='sp',
                 output_dir='./', collect_invalid = False, *args, **kwargs):

        super(self.__class__, self).__init__(*args, **kwargs)
        self.server_name = server_name
        self.server_type = server_type
        self.logfile_path = logfile_path  # 日志所在目录
        self.collect_invalid = collect_invalid
        self.month = month  # 目标月份
        self.regulars = LOG_REGULAR_EXPRESSIONS.get(self.server_name).get(self.server_type)  # 获取正则表达式
        self.log_regx_obj = re.compile(self.regulars.get('pattern')) # 预编译正则
        self.output_dir = output_dir  # 错误、警告等信息记录文件路径
        self.container = {}  # 每日统计载体
        self.stat_body = {}  # 最终结果载体
        self.check_args()

    def check_args(self):
        if not (isinstance(self.month, int) and (0 < self.month <= 12)):
            raise AttributeError('月份参数错误!')
        if not (self.server_name in ALLOWED_SERVER_NAMES and self.server_type in ALLOWED_SERVER_TYPES):
            raise AttributeError('项目名/类型参数错误!')
        if not os.path.exists(self.logfile_path):
            raise AttributeError('log目录错误！')

    def check_date(self, result):
        """检测目标记录月份是否匹配"""
        is_current_month = True
        try:
            date_time = result.group('datetime')
            date_time = date_time.split(' ')[0]
            if not datetime.strptime(date_time, '%d/%b/%Y:%H:%M:%S').month == self.month:
                is_current_month = False
        except AttributeError:
            is_current_month = False
            date_time = None
        return is_current_month, date_time

    def parse_request(self, request):
        # 解析request部分
        # 有部分url_part是需要忽略的，如77.72.83.87 - - [12/Jan/2019:10:50:43 +0000] "\x03" 400 226 "-" "-"
        try:
            url = request.split(' ')[1]
        except IndexError:
            return None

        effective_count = route_count = diagram_count = fare_count = 0

        if url in self.regulars.get('index_pattern'):
            effective_count = 1

        # if url == '/':
        #     effective_count = 1

        for route_pattern in self.regulars.get('route_patterns'):
            route_search_result = re.search(route_pattern, url)
            if route_search_result:
                effective_count = 1
                route_count = 1

        for diagram_pattern in self.regulars.get('diagram_patterns'):
            diagram_search_result = re.search(diagram_pattern, url)
            if diagram_search_result:
                diagram_count = 1
                effective_count = 1

        for fare_pattern in self.regulars.get('fare_patterns'):
            fare_search_result = re.search(fare_pattern, url)
            if fare_search_result:
                fare_count = 1
                effective_count = 1

        return effective_count, route_count, diagram_count, fare_count

    def parse_group(self, log_record):
        try:
            result = self.log_regx_obj.search(log_record)
            return result
        except AttributeError:
            return None

    def store_into_container(self, date_time, request):
        """每个log文件都要把相应结果存入容器用于最终统计"""
        date = datetime.strptime(date_time, '%d/%b/%Y:%H:%M:%S')  # 获取日志对应日期
        date_str = date.strftime('%Y/%m/%d')  # 获取指定格式的日期字符串
        weekday_name = JAPANESE_WEEKDAY_NAMES.get(date.weekday())  # 获取该日期对应星期几

        try:
            effective_count, route_count, diagram_count, fare_count = self.parse_request(request)
        except:
            pass
        else:
            if not self.container.get(date_str):
                self.container[date_str] = [date_str, weekday_name, effective_count, route_count, diagram_count,
                                            fare_count]
            else:
                self.container[date_str][2] += effective_count
                self.container[date_str][3] += route_count
                self.container[date_str][4] += diagram_count
                self.container[date_str][5] += fare_count

    def get_fukuoka_result(self):
        """统计最终结果"""
        date_list = []  # 日期字符串
        total_list = []  # 总访问量
        route_list = []  # 经路检索访问量
        diagram_list = []  # 时刻表访问量
        fare_list = []  # 料金检索访问量

        # 统计每类工作日的访问量
        weekday_stat = {
            '日曜日': 0,
            '月曜日': 0,
            '火曜日': 0,
            '水曜日': 0,
            '木曜日': 0,
            '金曜日': 0,
            '土曜日': 0,
        }

        daily = {}

        for daily_record in sorted(self.container.values()):
            date_str, weekday_name, count, route, diagram, fare = daily_record

            count = int(count)

            # 统计曜日数据
            weekday_stat[weekday_name] += count

            # 统计每天数据
            daily[date_str] = count

            date_list.append(date_str)
            total_list.append(count)
            route_list.append(route)
            diagram_list.append(diagram)
            fare_list.append(fare)

        # 统计总的访问量、三大模块访问量
        total = sum([int(item) for item in total_list])
        total_route = sum([int(item) for item in route_list])
        total_diagram = sum([int(item) for item in diagram_list])
        total_fare = sum([int(item) for item in fare_list])

        self.stat_body['total_stat'] = total
        self.stat_body['route_stat'] = total_route
        self.stat_body['diagram_stat'] = total_diagram
        self.stat_body['fare_stat'] = total_fare
        self.stat_body['weekday_stat'] = weekday_stat
        self.stat_body['daily_stat'] = daily

        json_file_name = '{}-{}-{}.json'.format(
            self.server_name, self.server_type, self.month
        )

        json_file = os.path.join(self.output_dir, json_file_name)
        # 若目标名文件已存在，先删除
        if os.path.exists(json_file):
            os.remove(json_file)

        json_file_obj = open(json_file, 'w', encoding='utf-8')
        json.dump(self.stat_body, json_file_obj, ensure_ascii=False)

    def get_unusual(self, file_name, log_record):
        """记录异常日志"""
        unusual_out_put_path = os.path.join(self.output_dir, 'unusual.txt')
        unusual = open(unusual_out_put_path, 'a', encoding='utf-8')
        path = os.path.abspath(file_name).replace('\\', '/')
        unusual.write('{}\n{}\n'.format(path, log_record))
        unusual.flush()
        unusual.close()

    def get_invalid(self, file_name, log_record):
        """纪录无效日志"""
        if self.collect_invalid:
            invalid_out_put_path = os.path.join(self.output_dir, 'invalid.txt')
            invalid = open(invalid_out_put_path, 'a', encoding='utf-8')
            path = os.path.abspath(file_name).replace('\\', '/')
            invalid.write('{}\n{}\n'.format(path, log_record))
            invalid.flush()
            invalid.close()
        else:
            pass

    def analyzer(self):

        """日志分析主函数"""
        file_list = []
        for root, dir, files in os.walk(self.logfile_path):
            for name in files:
                file_list.append(os.path.join(root, name))

        for file in file_list:
            with open(file, 'rb') as log_file:
                while True:
                    log_record = log_file.readline().decode('utf-8')

                    if not log_record:
                        break

                    # 正则的分组解析
                    result = self.parse_group(log_record)

                    # 跳过没有正确解析出组件的的记录并记录之(若开启记录功能)
                    if not result:
                        self.get_invalid(file, log_record)
                        continue

                    # 跳过状态码不是200的记录
                    if not int(result.group('status')) == 200:
                        continue

                    # 跳过无日期或者非指定月份的记录
                    is_current_month, date_time = self.check_date(result)
                    if not (is_current_month and date_time):
                        continue

                    # 处理request部分
                    # 跳过没有找到request组件的log记录
                    try:
                        request = result.group('request')
                    except AttributeError:
                        self.get_unusual(file, log_record)
                        continue
                    else:
                        self.store_into_container(date_time, request)

        # 执行统计并输出json文件
        self.get_fukuoka_result()


if __name__ == '__main__':

    # 请根据LogAnalyzer参数说明进行实例化！
    log_analyzer = LogAnalyzer(
        logfile_path='./logs/fukuoka_1902',  # 日志目录
        month=2,  # 目标月份
        server_name='fukuoka',  # 项目名
        server_type='sp',  # 主机类型。sp表示传统机
        output_dir='./output',  # 结果输出文件夹
        collect_invalid = False  # 是否输出非有效访问的log记录
    )
    log_analyzer.analyzer()
