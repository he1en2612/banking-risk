import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 1. Load cấu hình từ file .env
load_dotenv() 

# Lấy URL từ file .env
db_url = os.getenv('DATABASE_URL')

# GIẢI PHÁP DỰ PHÒNG: Nếu file .env bị lỗi không đọc được, hãy dán trực tiếp URL vào đây
if db_url is None:
    db_url = "postgresql://neondb_owner:npg_15TRGpcbfgoZ@ep-royal-bar-a1xbionq-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

def upload_sample_data():
    try:
        print("--- Đang chuẩn bị dữ liệu ---")
        # 2. Tạo dữ liệu giả lập rủi ro tín dụng (Banking Risk Data)
        data = {
            'customer_id': range(101, 111),
            'customer_name': ['An', 'Binh', 'Chi', 'Dung', 'Em', 'Giang', 'Hoa', 'Ien', 'Khanh', 'Lam'],
            'loan_amount': [5000, 12000, 8000, 25000, 3000, 15000, 7000, 9000, 30000, 4500],
            'income': [1500, 2000, 1800, 3500, 1200, 2500, 1600, 1700, 4000, 1400],
            'risk_rating': ['Low', 'High', 'Medium', 'High', 'Low', 'Medium', 'Low', 'Low', 'High', 'Low'],
            'days_overdue': [0, 45, 15, 60, 0, 20, 0, 5, 90, 0] # Số ngày quá hạn
        }
        df = pd.DataFrame(data)
        
        # 3. Kết nối tới Neon Cloud
        print(f"--- Đang kết nối tới Database ---")
        engine = create_engine(db_url)
        
        # 4. Đẩy dữ liệu lên bảng 'credit_risk_samples'
        # if_exists='replace' sẽ tự tạo bảng mới nếu chưa có, hoặc ghi đè nếu đã có
        df.to_sql('credit_risk_samples', engine, if_exists='replace', index=False)
        
        print("------------------------------------------")
        print("🚀 THÀNH CÔNG! Dữ liệu đã nằm trên Cloud Database.")
        print("Bây giờ bạn có thể mở DBeaver để kiểm tra bảng 'credit_risk_samples'.")
        print("------------------------------------------")

    except Exception as e:
        print(f"❌ LỖI XẢY RA: {e}")

if __name__ == "__main__":
    upload_sample_data()