import csv
import os

def clean_csv_files(input_folder, output_folder, file_extension):
    for filename in os.listdir(input_folder):
        if filename.endswith(file_extension):
            input_file = os.path.join(input_folder, filename)
            output_file = os.path.join(output_folder, filename)
            clean_csv(input_file, output_file)

def clean_csv(input_file, output_file):
    with open(input_file, 'r') as file_in, open(output_file, 'w', newline='') as file_out:
        reader = csv.reader(file_in)
        writer = csv.writer(file_out)

        for row in reader:
            # 检查并清理每行末尾的额外字符"|"
            cleaned_row = [cell.rstrip('|') for cell in row]
            writer.writerow(cleaned_row)



if __name__ == "__main__":
    input_folder = '.'  # 当前文件夹
    output_folder = 'clean'
    file_extension = '.tbl'
    
    clean_csv_files(input_folder, output_folder, file_extension)
    print("CSV文件已成功清理！")
