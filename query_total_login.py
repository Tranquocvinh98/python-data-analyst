import psycopg2
from psycopg2 import sql
import config as cf
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import schedule
import time
def query_db(date_start, date_end):
    conn = psycopg2.connect(
        dbname=cf.data_base_source,
        user=cf.user_source,
        password=cf.password_source,
        host=cf.host_source,
        port="5432"
    )
    cur = conn.cursor()
    query = """SELECT DATE(login_date), count(distinct(customer_id))
               FROM customer_activity_2024
                WHERE DATE(login_date) >= \'{}\' and DATE(login_date) <= \'{}\'
               group by DATE(login_date)
            """.format(date_start, date_end)
    try:
        cur.execute(query)
        results = cur.fetchall()
        return results
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0
    finally:
        cur.close()
        conn.close()

def extract_date():
    date_start = "2024-09-01"
    conn = psycopg2.connect(
        dbname=cf.data_base_target,
        user=cf.user_target,
        password=cf.password_target,
        host=cf.host_target,
        port="5432"
    )
    cur = conn.cursor()
    query = """ 
    SELECT MAX(date) AS max_login_date
    FROM total_customer_activity_2024;
    """
    try:
        cur.execute(query)
        results = cur.fetchall()
        if results[0][0] is not None:
            date_start= results[0][0]
            date_start = date_start + timedelta(days=1)
            date_start = date_start.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()
    current_time = datetime.now()
    date_yesterday = current_time - timedelta(days=1)
    date_end = date_yesterday.strftime("%Y-%m-%d")
    return date_start, date_end

def insert_into_db(results):
    # Kết nối tới cơ sở dữ liệu PostgreSQL
    conn = psycopg2.connect(
        dbname=cf.data_base_target,
        user=cf.user_target,
        password=cf.password_target,
        host=cf.host_target,
        port="5432"
    )

    # Tạo đối tượng con trỏ
    cur = conn.cursor()

    # Câu lệnh SQL để chèn dữ liệu
    insert_query = sql.SQL(
        "INSERT INTO total_customer_activity_2024 (date, total_login) VALUES %s"
    )

    # Chèn dữ liệu vào bảng
    execute_values(cur, insert_query, results)

    # Xác nhận thay đổi
    conn.commit()

    # Đóng con trỏ và kết nối
    cur.close()
    conn.close()

    print("Dữ liệu đã được chèn thành công.")


def job():
    date_start, date_end = extract_date()
    result = query_db(date_start, date_end)
    insert_into_db(result)

if __name__ == '__main__':
    schedule.every().day.at(cf.time_run_job).do(job)
    while True:
        schedule.run_pending()
        time.sleep(60)  # Kiểm tra mỗi phút

