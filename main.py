import pandas as pd
import numpy as np
from random import randint, choice
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Cài đặt số dòng của bảng
num_rows = 1000000
num_ids = 1000000

# Tạo dữ liệu cho cột id theo thứ tự từ 1 đến 1,000,000
ids = np.random.randint(1, 1501, size=num_ids)

# Tạo dữ liệu cho cột số tiền giao dịch (giả sử từ 100 đến 10,000)
transaction_amounts = np.random.randint(100, 10000, size=num_rows)

# Tạo dữ liệu cho cột ngày giao dịch theo thời gian trong năm 2023
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
date_range = (end_date - start_date).days + 1
transaction_dates = [start_date + timedelta(days=i % date_range) for i in range(num_rows)]

# Tạo DataFrame
df = pd.DataFrame({
    'id': ids,
    'số tiền giao dịch': transaction_amounts,
    'ngày giao dịch': transaction_dates
})

# Sắp xếp DataFrame theo ngày giao dịch
df = df.sort_values(by='ngày giao dịch').reset_index(drop=True)

# Hiển thị một số dòng đầu tiên của DataFrame
print(df.head())

# Kết nối tới PostgreSQL (thay đổi thông tin kết nối tùy theo hệ thống của bạn)
engine = create_engine("postgresql://tranquocvinh:13579@localhost:5432/data-analyst")

# Lưu dữ liệu vào PostgreSQL
df.to_sql("customer", engine, if_exists="replace", index=False)

print("Dữ liệu đã được lưu vào PostgreSQL thành công.")


