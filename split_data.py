import pandas as pd
from sklearn.model_selection import train_test_split
import re

print("Đang đọc file gốc...")
df = pd.read_csv('training.1600000.processed.noemoticon.csv', encoding='latin-1', header=None)

# Tiền xử lý nhẹ để tăng Accuracy
print("Đang tiền xử lý...")
df[5] = df[5].apply(lambda x: re.sub(r'http\S+|@\w+|\n|\r', '', str(x).lower()).strip())

print("Đang lấy mẫu và chia dữ liệu...")
# Lấy 150k dòng ngẫu nhiên để đảm bảo cân bằng nhãn và nhẹ máy
df_sample = df.sample(n=150000, random_state=42)

train_df, test_df = train_test_split(df_sample, test_size=20000, random_state=42)

# Lưu file - Sử dụng quoting để bảo vệ dữ liệu văn bản
train_df.to_csv('train_split.csv', index=False, header=None, quoting=1)
test_df.to_csv('test_split.csv', index=False, header=None, quoting=1)

print(f"Hoàn thành! Train: {len(train_df)} dòng, Test: {len(test_df)} dòng.")