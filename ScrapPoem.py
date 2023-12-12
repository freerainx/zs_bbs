import datetime
import logging
import sys
from msvcrt import kbhit, getch
from time import sleep

from pymysql.converters import escape_string

import sqlite3
import requests
from bs4 import BeautifulSoup, Tag

from MysqlDb import *
from MysqlDbEx import *

from selenium import webdriver

# -----------------------------------------------------------------------------------------------
DB_FILE = 'E:\\Work\\DB\\zs_bbs.db'
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "main",
    "charset": "utf8mb4",
    "user": "root",
    "passwd": "rain1109"
}

sql_db = None
use_sqlite = True

lite_conn: sqlite3.Connection
lite_cursor: sqlite3.Cursor


# -----------------------------------------------------------------------------------------------


def isKeyPressed(_key: bytes) -> bool:
    if kbhit():
        key = getch()
        if key == _key:
            return True

    return False


# -----------------------------------------------------------------------------------------------


def OpenDB() -> bool:
    if use_sqlite:
        global lite_conn
        lite_conn = sqlite3.connect(DB_FILE)
        global lite_cursor
        lite_cursor = lite_conn.cursor()
    else:
        try:
            global sql_db
            sql_db = MysqlDbEx(**DB_CONFIG)
        except ValueError as e:
            # print("connect db error.")
            print(str(e))
            return False

    return True


def RollbackDB():
    if use_sqlite:
        lite_conn.rollback()


def CommitDB():
    if use_sqlite:
        lite_conn.commit()


def CloseDB():
    if use_sqlite:
        global lite_conn
        lite_conn.close()


def exist_poem_list_lite(name, category, poet) -> bool:
    # sql = "select * from poem_list where Name = '{}' and Category = '{}' and Poet ='{}'".format(name, category, poet)
    # print(sql)

    sql = "select * from poem_list where Name = ? and Category = ? and Poet = ?"
    try:
        global lite_cursor
        lite_cursor = lite_conn.cursor()
        lite_cursor.execute(sql, (name, category, poet))
        records = lite_cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(sql)
        return False

    return len(records) > 0


def save_poem_list_lite(name, category, poet, create_date, feedback_cnt, review_cnt, href) -> bool:
    if exist_poem_list_lite(name, category, poet):
        return True

    sql = "INSERT INTO poem_list (Name,Category,Poet,CreateDate,FeedbackCnt,ReviewCnt,href) VALUES (?,?,?,?,?,?,?)"
    try:
        lite_conn.execute(sql, (name, category, poet, create_date, feedback_cnt, review_cnt, href,))
    except sqlite3.OperationalError as e:
        print(sql)
        return False

    return True


def exist_poem_lite(poem_title, poem_poet) -> bool:
    # sql = "select * from poem_info where Name = '{}' and Poet ='{}'".format(poem_title, poem_poet)
    sql = "select * from poem_info where Name = ? and Poet = ?"
    try:
        global lite_cursor
        lite_cursor = lite_conn.cursor()
        lite_cursor.execute(sql, (poem_title, poem_poet))
        records = lite_cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(sql)
        return False

    return len(records) > 0


def save_poem_info_lite(poem_title, poem_poet, poem_time, poem_content) -> bool:
    if exist_poem_lite(poem_title, poem_poet):
        return True
    sql = "INSERT INTO poem_info (Name,Poet,CreateTime,Content) VALUES (?, ?, ?, ?)"

    try:
        lite_cursor.execute(sql, (poem_title, poem_poet, poem_time, poem_content,))
    except sqlite3.OperationalError as e:
        print(sql)
        return False

    return True


def save_poem_list(name, category, poet, create_date, feedback_cnt, review_cnt, href) -> bool:
    return sql_db.insert("poem_list", (0, name, category, poet, create_date, feedback_cnt, review_cnt, href))


def save_poem_info(poem_title, poem_poet, poem_time, poem_content) -> bool:
    return sql_db.insert("poem_info", (0, 0, poem_title, poem_poet, poem_time, poem_content.strip()))


# ***********************************************************************************************************


def scrape_poem_list(page: int, fromDate: str = "2000-01-01 00:00:00") -> (bool, int):
    url = "https://bbs.yzs.com/forum-4-" + str(page) + ".html"
    print("scraping: " + url)

    # *[ @ id = "threadlisttableid"]
    # *[ @ id = "moderate"]

    try:
        wbdata = requests.get(url).text
    except Exception as e:
        print(str(e))
        print("fail to connect url.")
        logger.debug(url)
        return False, 1

    soup = BeautifulSoup(wbdata, 'lxml')
    # print(soup)
    # 从解析文件中通过select选择器定位指定的元素，返回一个列表
    poem_list = soup.select("#threadlisttableid > tbody")
    # print(poem_list)

    if len(poem_list) == 0:
        logger.debug(url)
        return False, 2

    count = 0
    res = 0
    for poem_item in poem_list[0:]:
        # print(poem_info)
        ID = poem_item.attrs['id']
        if ID in ["separatorline", "forumnewshow"]:
            continue

        str_details = BeautifulSoup(str(poem_item), 'lxml').select('tr')
        details = BeautifulSoup(str(str_details), 'lxml')

        try:
            icn_title = details.select('tr > .icn > a')[0]['title']
            if icn_title.find("置顶") != -1:
                continue

            category = details.select('th > em > a')[0].text
            name = details.select('th > .xst')[0].text
            href = details.select('th > .xst')[0]['href']
            feedback_cnt = details.select('.num > a')[0].text
            review_cnt = details.select('.num > em')[0].text
            list_by = details.select('.by')

            poet = list_by[0].select('cite > a')[0].text
            if len(list_by[0].select('em > span > span')) == 0:
                create_date = list_by[0].select('em > span')[0].text
            else:
                create_date = list_by[0].select('em > span > span')[0]['title']

            feedback_poet = list_by[1].select('cite > a')[0].text
            if len(list_by[1].select('em > a > span')) == 0:
                feedback_date = list_by[1].select('em > a')[0].text
            else:
                feedback_date = list_by[1].select('em > a > span')[0]['title']

        except ValueError as e:
            print(str(e))
            continue
        except IndexError as e:
            print(str(e))
            continue

        try:
            dtDate = datetime.datetime.strptime(create_date, '%Y-%m-%d')
            create_date = dtDate.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            print(str(e))
            continue

        try:
            dtDate = datetime.datetime.strptime(feedback_date, '%Y-%m-%d %H:%M')
            feedback_date = dtDate.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            print(str(e))
            continue

        if feedback_date < fromDate:
            res = 9
            break

        if create_date < fromDate:
            continue

        if use_sqlite:
            # name=escapeString
            save_poem_list_lite(name, category, poet, create_date, feedback_cnt, review_cnt, href)
        else:
            name = escape_string(name)
            save_poem_list(name, category, poet, create_date, feedback_cnt, review_cnt, href)
        count += 1

        # print(category, end=" ")
        # print(name, end=" ")
        # print(poet, end=" ")
        # print(create_date, end=" ")
        # print(feedback_cnt, end=" ")
        # print(review_cnt, end=" ")
        # print(href)

        # print("******************************************************")

    CommitDB()
    return True, res


def scrape_poem(url) -> bool:
    """
    https://bbs.yzs.com/thread-1091026-1-1.html
    https://bbs.yzs.com/forum.php?mod=viewthread&tid=1091026&extra=page%3D1
    """

    # print("==========================================================")
    print("scraping: " + url)

    try:
        wbdata = requests.get(url).text
    except Exception as e:
        print(str(e))
        print("fail to connect url.")
        return False

    soup = BeautifulSoup(wbdata, 'lxml')
    # print(type(soup))
    # print(type(soup.select("#thread_subject")))

    try:
        review_cnt = 0
        feedback_cnt = 0

        topic_pages = soup.select("#pgt > .pgt > .pg > label > span")
        page_cnt = 1
        cur_page = 1
        if topic_pages:
            txt = topic_pages[0].text
            txt = txt.replace(' 页', '').replace('/', '').lstrip().rstrip()
            page_cnt = int(txt)

            txt = soup.select("#pgt > .pgt > .pg > label > input")[0]['value']
            cur_page = int(txt)

        # print("pages: " + str(page_cnt))
        # print("cur_page: " + str(cur_page))

        # ----------------------------------------------------------------------
        post_list = soup.find('div', id="postlist")
        if post_list is None:
            return False

        # 没有找到帖子 这里会出错
        cnt_list = post_list.select('.ptn > .xi1')
        review_cnt = int(cnt_list[0].text)
        feedback_cnt = int(cnt_list[1].text)

        poem_title = post_list.find('span', id='thread_subject').text
        # ----------------------------------------------------------------------

        div_cnt = len(post_list.contents)
        i = 0
        loft_list = []
        for item in post_list.children:
            # print(type(item)) # bs4.element.NavigableString
            if type(item) is not Tag:
                continue
            if item.name != 'div':
                continue

            item_id = item.attrs['id']
            if item_id == 'postlistreply':
                continue
            if item_id == 'hiddenpoststip':
                continue
            if item_id == 'hiddenposts':
                continue

            item_id = item_id.replace("post_", "")
            # print(item_id)

            loft = {'poet': item.select(".pi > .authi > .xw1")[0].text}

            time_id = "#authorposton" + item_id

            if len(item.select("{} > span".format(time_id))) > 0:
                loft['time'] = item.select("{} > span".format(time_id))[0]['title']
            else:
                loft['time'] = item.select(time_id)[0].text
                loft['time'] = loft['time'].replace("发表于 ", "")

            # if len(item.select(".pti > .authi > em > span")) > 0:
            #     loft['time'] = item.select(".pti > .authi > em > span")[0]['title']
            # else:
            #     loft['time'] = item.select(".pti > .authi > em")[0].text
            #     loft['time'] = loft['time'].replace("发表于 ", "")

            dtDate = datetime.datetime.strptime(loft['time'], "%Y-%m-%d %H:%M:%S")
            loft['time'] = dtDate.strftime("%Y-%m-%d %H:%M:%S")

            if item.find("div", class_="locked") is not None:
                return True

            # poem_content = soup.select("tbody > tr > .t_f")[0].text
            loft['content'] = item.find("td", class_="t_f").text

            # ----------------------------------------------------
            start_pos = loft['content'].find("本帖最后由")
            end_pos = 0
            if start_pos != -1:
                end_pos = loft['content'].find('\n', start_pos)
                if end_pos != -1:
                    s = loft['content'][start_pos:end_pos + 1]
                    loft['content'] = loft['content'].replace(s, '')

            loft['content'] = loft['content'].replace('\xa0', '')

            # ----------------------------------------------------
            loft_list.append(loft)
            i = i + 1

            # print("--------------------------第 {} 楼--------------------------".format(i))
            # print(loft['poet'])
            # print(loft['time'])
            # print(loft['content'])

    except IndexError as e:
        print(str(e))
        logger.debug(url)
        return False
    except ValueError as e:
        print(str(e))
        logger.debug(url)
        return False
    except AttributeError as e:
        print(str(e))
        logger.debug(url)
        return False

    # print("----------------------------------------------------------")
    # print(poem_title)
    # print(review_cnt, feedback_cnt)
    # print("----------------------------------------------------------")

    if cur_page == 1:
        if use_sqlite:
            save_poem_info_lite(poem_title, loft_list[0]['poet'], loft_list[0]['time'], loft_list[0]['content'])
            pass
        else:
            poem_title = escape_string(poem_title)
            save_poem_info(poem_title, loft_list[0]['poet'], loft_list[0]['time'], loft_list[0]['content'])

    return True


def scrape_poet(poet) -> bool:
    #
    # ID,
    # Name,
    # Category,
    # Poet,
    # CreateDate,
    # FeedbackCnt,
    # ReviewCnt,
    # href
    #
    lite_cursor.execute("SELECT Name, CreateDate, href FROM poem_list where poet=? ", (poet,))

    allData = lite_cursor.fetchall()

    for item in allData:
        strName = item[0]
        strDate = item[1]
        print(strDate, strName)

        strUrl = item[2]

        scrape_poem(strUrl)

    return True


def scrape_all_poem(start_time) -> bool:
    sql = "SELECT Name, CreateDate, href FROM poem_list where CreateDate>='{}' order by CreateDate"
    sql = sql.format(start_time)
    lite_cursor.execute(sql)

    allData = lite_cursor.fetchall()

    total_cnt = len(allData)
    print("total poem amount: ", total_cnt)

    cnt = 0
    for item in allData:
        strName = item[0]
        strDate = item[1]

        print("{}/{} [{}] {}".format(cnt + 1, total_cnt, strDate, strName))

        strUrl = item[2]
        scrape_poem(strUrl)
        cnt = cnt + 1
        if cnt % 50 == 0:
            CommitDB()

        # -----------------------------------------------
        if isKeyPressed(b'q'):
            chs = input("exit ? (y/n)")
            if chs == 'y':
                break
        # -----------------------------------------------
    CommitDB()
    return True


def export_all_poems(poet, fromDate) -> bool:
    if poet == "---":
        sql = "SELECT Name, Poet, CreateTime, Content FROM poem_info where CreateTime>='{}' order by CreateTime"
        sql = sql.format(fromDate)
        filename = "AllPoet.txt"
    else:
        sql = "SELECT Name, Poet, CreateTime, Content FROM poem_info where Poet='{}' and CreateTime>='{}' order by CreateTime"
        sql = sql.format(poet, fromDate)
        filename = poet + ".txt"

    lite_cursor.execute(sql)
    # print(sql)
    allPoem = lite_cursor.fetchall()

    if len(allPoem) == 0:
        print("Poet or poem is not found.")
        return False

    print("Total amount of poems: ", len(allPoem))

    with open(filename, "w", encoding='UTF-8') as f:
        index = 1
        for poem in allPoem:
            print("\r%05d" % index, end="")
            poem_title = poem[0]
            poem_poet = poem[1]
            poem_time = poem[2]
            poem_text = poem[3]
            header = "[%05d]: [%s] [%s] %s\n" % (index, poem_poet, poem_time, poem_title)

            f.write("\n--------------------------------------------------------------------------\n")
            f.write(header)
            f.write("--------------------------------------------------------------------------\n")
            f.write(poem_text + "\n")

            index = index + 1

    return True


# ***********************************************************************************************************


def ChooseDB():
    global use_sqlite
    use_sqlite = True

    # while True:
    #     print("\n Welcome to zs-bbs scraper:")
    #     chs = input("select database (0=sqlite other=mysql) : ") or '0'
    #     if not chs.isdigit():
    #         continue
    #     global use_sqlite
    #     use_sqlite = (int(chs) == 0)
    #     break


def task_scrapeList():
    chs = input("input start page:")
    if not chs.isdigit():
        print("Please enter a number.")
        return

    if int(chs) > 1001 or int(chs) < 0:
        print("page number must between 1-1001.")
        return

    start_page = int(chs)

    chs = input("input end page:")
    if not chs.isdigit():
        print("Please enter a number.")
        return

    if int(chs) > 1000 or int(chs) < 0:
        print("page number must between 1-1001.")
        return

    end_page = int(chs)

    if end_page < start_page:
        print("end page must not be less than start page.")
        return

    for pg in range(start_page, end_page + 1):
        res, code = scrape_poem_list(pg)
        if not res:
            errmsg = "ERROR {}!!!".format(code)
            print(errmsg)
            logger.debug(errmsg)


def task_scrapeList_fromDate():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    fromDate = input("Please input start date (default: {})):".format(today)) or today

    try:
        dtDate = datetime.datetime.strptime(fromDate, '%Y-%m-%d')
        fromDate = dtDate.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print("time format error.")
        # print(str(e))
        return

    for pg in range(1, 1001):
        res, code = scrape_poem_list(pg, fromDate)
        if code == 9:
            break


def task_scrapePoem():
    chs = input("Please input href of poem: ")
    if scrape_poem(chs):
        CommitDB()
        print("success !!!")
    else:
        print("Wrong !!!")


def task_scrape_Poet():
    chs = input("Please poet's name: ")
    if scrape_poet(chs):
        CommitDB()
        print("success !!!")
    else:
        print("Wrong !!!")


def task_scrape_all_poem():
    start_time = input("please input start time:") or datetime.datetime.now().strftime("%Y-%m-%d")

    try:
        dtDate = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        start_time = dtDate.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print("time format error.")
        # print(str(e))
        return

    print("scrape all poems from : ", start_time)

    if input("are you sure ?(y/n)") != 'y':
        return

    scrape_all_poem(start_time)


def task_scrape_poems_list() -> int:
    res = 0
    file_name = input("Please input list file name: ")
    with open(file_name) as f:
        while True:
            line = f.readline()
            if len(line) == 0:
                break
            pos = line.find("https")
            if pos == -1:
                continue
            line = line[pos:]
            line = line.rstrip().rstrip('\n')
            # print(line)
            if scrape_poem(line):
                res = res + 1

    if res > 0:
        CommitDB()
    print("Total scraped poems: {}".format(res))
    return res


def task_export_all_poems():
    poet = input("please input poet's name, ---  means all poets: ")

    if not poet:
        print("you must input poet's name or --- means all poets")
        return

    today = "2000-01-01"
    fromDate = input("Please input start date (default: {})):".format(today)) or today

    try:
        dtDate = datetime.datetime.strptime(fromDate, '%Y-%m-%d')
        fromDate = dtDate.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print("date format error.")
        # print(str(e))
        return

    export_all_poems(poet, fromDate)

    pass


# ***********************************************************************************************************


def start():
    # ----------------------------------------------------------------
    ChooseDB()
    if not OpenDB():
        sys.exit()
    # ----------------------------------------------------------------
    while True:
        print()
        print("[scrape options:]")
        print("1 - scrape list [Start&End Page]")
        print("2 - scrape list from Date [Date]")
        print("3 - scrape poem [href]")
        print("4 - scrape all poems of poet [Poet]")
        print("5 - scrape all poems [Date]")
        print("6 - scrape from href-list file [File]")
        print("7 - export all poem form DB [Poet&Date]")
        print("0 - exit")
        print()

        chs = input("your choice : ")
        if not chs.isdigit():
            print("Please enter a number.")
            continue

        if int(chs) == 0:
            break
        elif int(chs) == 1:
            task_scrapeList()
        elif int(chs) == 2:
            task_scrapeList_fromDate()
        elif int(chs) == 3:
            task_scrapePoem()
        elif int(chs) == 4:
            task_scrape_Poet()
        elif int(chs) == 5:
            task_scrape_all_poem()
        elif int(chs) == 6:
            task_scrape_poems_list()
        elif int(chs) == 7:
            task_export_all_poems()
        else:
            print("Invalid choice.")
            continue

    # ----------------------------------------------------------------
    CloseDB()


def Test():
    if not OpenDB():
        sys.exit()

    # ----------------------------------------------------------------
    for p in range(2, 3):
        if not scrape_poem_list(p):
            break
    # ----------------------------------------------------------------
    poem_url = "https://bbs.yzs.com/thread-1095857-1-1.html"
    scrape_poem(poem_url)
    # ----------------------------------------------------------------
    # select_all = sql_db.select_all("poem_info", "ID>7")
    # for r in select_all:
    #     for w in r:
    #         print(w)

    # ----------------------------------------------------------------

def run_chrome():
    # browser = webdriver.Chrome()
    browser = webdriver.Firefox()
    browser.get('https://www.google.com/')



if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG,
    #                     filename='./logs.log',
    #                     filemode='a',
    #                     format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
    #                     datefmt='%Y-%m-%d %T'
    #                     )

    # logging.basicConfig(level=logging.DEBUG, datefmt='%m-%d %T')

    # 创建logger对象
    logger = logging.getLogger('test_logger')

    # 设置日志等级
    logger.setLevel(logging.DEBUG)

    # 追加写入文件a ，设置utf-8编码防止中文写入乱码
    test_log = logging.FileHandler('errPage.log', 'a', encoding='utf-8')

    # 向文件输出的日志级别
    test_log.setLevel(logging.DEBUG)

    # 向文件输出的日志信息格式
    # formatter = logging.Formatter('%(asctime)s - %(filename)s - line:%(lineno)d - %(levelname)s - %(message)s -%(process)s')
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%m-%d %T')
    test_log.setFormatter(formatter)

    # 加载文件到logger对象中
    logger.addHandler(test_log)

    logger.debug('××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××')
    # logger.info('[info]')
    # logger.warning('警告信息[warning]')
    # logger.error('错误信息[error]')
    # logger.critical('严重错误信息[crtical]')
    # ====================================================================================================

    # Test()
    # start()
    run_chrome()
