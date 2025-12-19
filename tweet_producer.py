import time
import pandas as pd
import argparse
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-file', type=str, required=True, help='Path to the CSV file')
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic name')
    parser.add_argument('--rate', type=int, default=1, help='Messages per second')
    args = parser.parse_args()

    # Kết nối Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: x.encode('utf-8')
        )
        print("Kết nối Kafka thành công!")
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        return

    print(f"Đang đọc file dữ liệu: {args.csv_file}...")
    
    # Đọc file dataset
    try:
        # Cấu trúc Sentiment140: 0:target, 1:ids, 2:date, 3:flag, 4:user, 5:text
        df = pd.read_csv(args.csv_file, encoding='latin-1', header=None)
    except Exception as e:
        print(f"Lỗi đọc file CSV: {e}")
        return

    print(f"Bắt đầu bắn tin nhắn CSV vào topic '{args.topic}'...")
    print("Format: ID,LABEL,DUMMY,TEXT")
    print("Nhấn Ctrl+C để dừng.\n")

    for index, row in df.iterrows():
        target = row.iloc[0]      # Cột 0: Label
        tweet_id = row.iloc[1]    # Cột 1: ID
        tweet_text = str(row.iloc[-1]) # Cột cuối: Nội dung

        # Xử lý nội dung: Xóa xuống dòng để tránh vỡ format CSV
        tweet_text = tweet_text.replace('\n', ' ').replace('\r', '')

        # TẠO CHUỖI CSV KHỚP VỚI JAVA
        # Java đọc: col[0]=ID, col[1]=Label, col[3]=Text. 
        # Nên ta chèn chữ "dummy" vào vị trí số 2.
        message = f"{tweet_id},{target},dummy,{tweet_text}"
        
        # Gửi sang Kafka
        producer.send(args.topic, value=message)
        
        print(f"Sent [{index}]: {message[:50]}...")
        
        time.sleep(1.0 / args.rate)

if __name__ == "__main__":
    main()