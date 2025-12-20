import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 1. Cấu hình môi trường
load_dotenv()
db_url = os.getenv('DATABASE_URL')

def etl_process():
    # 2. Đọc dữ liệu từ file CSV
    file_path = 'data/raw/german_credit_data.csv'
    
    if not os.path.exists(file_path):
        print(f"❌ Không tìm thấy file tại {file_path}. Hãy kiểm tra lại thư mục 'data'!")
        return

    # Đọc file và bỏ cột index thừa nếu có
    df = pd.read_csv(file_path, index_col=0)

    # 3. CHUẨN HÓA TÊN CỘT TRƯỚC (Để tránh lỗi KeyError)
    # Chuyển về chữ thường, thay khoảng trắng bằng dấu gạch dưới
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    print(f"✅ Các cột đã nhận diện: {df.columns.tolist()}")

    # 4. XỬ LÝ DỮ LIỆU (Feature Engineering & Cleaning)
    
    # Điền giá trị thiếu (Handling Nulls)
    # Lưu ý: Cột gốc là 'Saving accounts' -> sau chuẩn hóa là 'saving_accounts' (có chữ s)
    if 'saving_accounts' in df.columns:
        df['saving_accounts'] = df['saving_accounts'].fillna('Unknown')
    
    if 'checking_account' in df.columns:
        df['checking_account'] = df['checking_account'].fillna('Unknown')

    # Phân nhóm tuổi (Age Segmentation)
    bins = [0, 25, 40, 60, 100]
    labels = ['Young', 'Adult', 'Senior', 'Elderly']
    df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels)

    # Phân loại quy mô khoản vay (Loan Size)
    df['loan_segment'] = df['credit_amount'].apply(
        lambda x: 'Small' if x < 2000 else ('Medium' if x < 5000 else 'Large')
    )

    # 5. ĐẨY DỮ LIỆU LÊN CLOUD
    try:
        engine = create_engine(db_url)
        # Ghi đè vào bảng 'cleaned_german_credit'
        df.to_sql('cleaned_german_credit', engine, if_exists='replace', index=False)
        print(f"🚀 THÀNH CÔNG! Đã nạp {len(df)} dòng dữ liệu 'SẠCH' lên bảng cleaned_german_credit.")
    except Exception as e:
        print(f"❌ Lỗi khi kết nối Database: {e}")

if __name__ == "__main__":
    etl_process()