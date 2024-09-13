import pandas as pd
import numpy as np
from random import randint, choice
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Số lượng dòng dữ liệu cần tạo
num_rows = 1000000

# Danh sách mã khách hàng (1500 khách hàng)
num_customers = 1500
customers = [f"customer_{i}" for i in range(1, num_customers + 1)]

# Ngày bắt đầu và ngày kết thúc trong năm 2024
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

# Tạo danh sách ngày giờ tăng dần trong toàn bộ năm 2024
def generate_sequential_dates(start, end, n):
    """Tạo danh sách ngày giờ tăng dần từ ngày start."""
    delta = end - start
    return [start + timedelta(seconds=int(i * delta.total_seconds() / n)) for i in range(n)]

# Tạo danh sách ngày giờ tăng dần
login_dates = generate_sequential_dates(start_date, end_date, num_rows)

# Tạo bảng dữ liệu với cột customer_id và login_date
data = {
    "customer_id": [choice(customers) for _ in range(num_rows)],
    "login_date": login_dates
}

# Tạo DataFrame từ dữ liệu
df = pd.DataFrame(data)

# Hiển thị vài dòng đầu của bảng dữ liệu
print(df.head())

# Lưu DataFrame vào tệp CSV
df.to_csv('customer_activity_2024.csv', index=False)

print("Dữ liệu đã được tạo và lưu vào tệp CSV thành công.")



# Kết nối tới PostgreSQL (thay đổi thông tin kết nối tùy theo hệ thống của bạn)
engine = create_engine("postgresql://tranquocvinh:13579@localhost:5432/data-analyst")

# Lưu dữ liệu vào PostgreSQL
df.to_sql("customer_activity_2024", engine, if_exists="replace", index=False)

print("Dữ liệu đã được lưu vào PostgreSQL thành công.")