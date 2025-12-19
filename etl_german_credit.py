import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL')

def etl_process():
    # 1. Đọc dữ liệu từ file CSV bạn vừa tải
    # Lưu ý: Sửa đường dẫn nếu bạn đặt file ở chỗ khác
    file_path = 'data/raw/german_credit_data.csv'
    
    if not os.path.exists(file_path):
        print(f"❌ Không tìm thấy file tại {file_path}. Hãy kiểm tra lại thư mục!")
        return

    df = pd.read_csv(file_path, index_col=0)

    # 2. Làm sạch tên cột (Bỏ khoảng trắng, chuyển về chữ thường để SQL dễ làm việc)
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # 3. Kết nối và đẩy dữ liệu
    engine = create_engine(db_url)
    df.to_sql('raw_german_credit', engine, if_exists='replace', index=False)
    
    print(f"🚀 Thành công! Đã nạp {len(df)} dòng dữ liệu German Credit lên Cloud.")

if __name__ == "__main__":
    etl_process()