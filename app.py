import streamlit as st
import pandas as pd
import json
import os
import re
import hashlib
import datetime
import zipfile
from collections import defaultdict
import io
import warnings

# 忽略不必要的警告
warnings.filterwarnings("ignore")

# --- 核心处理函数 (与桌面版基本一致) ---

def clean_cell_value(value):
    """清洗单元格值：处理空值、去空格，并为数字保留6位小数"""
    if pd.isna(value) or value == '':
        return ""
    return str(value).strip()

def get_file_md5(file_path):
    """计算文件的MD5校验码（用于本地文件）"""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return ""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def get_bytes_md5(bytes_data):
    """计算字节流的MD5校验码（用于内存中的文件）"""
    if not bytes_data:
        return ""
    md5_hash = hashlib.md5()
    md5_hash.update(bytes_data)
    return md5_hash.hexdigest()

def read_task_info(df_task):
    """读取「任务说明」sheet，获取社会统一信用代码和数据日期"""
    if df_task.empty:
        raise ValueError("「任务说明」sheet无有效数据")

    required_cols = ["社会统一信用代码", "数据日期"]
    missing_cols = [col for col in required_cols if col not in df_task.columns]
    if missing_cols:
        raise ValueError(f"「任务说明」sheet缺少必要列：{', '.join(missing_cols)}")

    first_row = df_task.iloc[0]
    credit_code = clean_cell_value(first_row["社会统一信用代码"])
    data_date_raw = clean_cell_value(first_row["数据日期"])

    data_date = data_date_raw.replace("-", "").strip()
    if not data_date:
        raise ValueError("数据日期为空或格式错误")

    return credit_code, data_date

def read_business_info(df_business):
    """读取「业务说明」sheet，获取业务表单名称和英文名称（去重）"""
    if df_business.empty:
        st.warning("「业务说明」sheet无有效数据，将生成默认空dat文件")
        return []

    required_cols = ["业务表单名称", "业务表单英文名称"]
    missing_cols = [col for col in required_cols if col not in df_business.columns]
    if missing_cols:
        raise ValueError(f"「业务说明」sheet缺少必要列：{', '.join(missing_cols)}")

    business_list = df_business[required_cols].drop_duplicates().to_dict("records")
    return business_list

def process_business_data(excel_file, business, check_group, credit_code, data_date, logs):
    """处理单个业务表单数据，生成DAT内容"""
    business_name = clean_cell_value(business["业务表单名称"])
    business_en_name = clean_cell_value(business["业务表单英文名称"])
    
    logs.append(f"\n--- 处理业务表单：{business_name}（英文名称：{business_en_name}）---")

    dat_strings = []
    
    # 检查是否有匹配的校验结果
    if check_group is None or business_name not in check_group.groups:
        logs.append(f"警告：无匹配的校验结果数据，生成空dat文件")
        return dat_strings, business_en_name

    check_rows = check_group.get_group(business_name)
    logs.append(f"找到{len(check_rows)}行匹配的校验结果数据")

    for _, check_row in check_rows.iterrows():
        # 生成第一段字符串
        c_val = clean_cell_value(check_row.iloc[2] if len(check_row) > 2 else "")
        i_val = clean_cell_value(check_row.iloc[8] if len(check_row) > 8 else "")
        g_val = clean_cell_value(check_row.iloc[6] if len(check_row) > 6 else "")
        part1 = f"01|{c_val}|{i_val}|{g_val}|"

        # 处理J列JSON数据
        j_val = clean_cell_value(check_row.iloc[9] if len(check_row) > 9 else "")
        if not j_val:
            logs.append(f"警告：J列无JSON数据，跳过该行")
            continue
        
        try:
            json_obj = json.loads(j_val)
            if not json_obj:
                logs.append(f"警告：JSON为空对象，跳过该行")
                continue
            
            json_key = list(json_obj.keys())[0]
            json_value = json_obj[json_key]

            if not isinstance(json_value, list) or len(json_value) == 0:
                logs.append(f"警告：JSON value不是非空数组，跳过该行")
                continue
            
            target_row_num = str(json_value[0]).strip()
        except json.JSONDecodeError:
            logs.append(f"警告：JSON解析失败，跳过该行")
            continue

        # 读取示例数据sheet
        sample_sheet_name = f"示例数据_{business_name}"
        part2 = ""
        try:
            df_sample = pd.read_excel(excel_file, sheet_name=sample_sheet_name, header=1, dtype=str)
            if df_sample.empty:
                logs.append(f"警告：示例数据sheet无有效数据")
                continue
            if len(df_sample.columns) < 3:
                logs.append(f"警告：示例数据sheet缺少C列")
                continue

            # 清洗C列用于匹配
            df_sample["C列_清洗后"] = df_sample.iloc[:, 2].apply(clean_cell_value)
            matched_rows = df_sample[df_sample["C列_清洗后"] == target_row_num]

            if matched_rows.empty:
                logs.append(f"警告：示例数据sheet中未找到C列='{target_row_num}'的行")
                continue
            
            # 删除临时列
            matched_rows = matched_rows.drop(columns=["C列_清洗后"])
            target_row = matched_rows.iloc[0]
            
            # 取D列及以后数据
            d_after_data = target_row.iloc[3:] if len(target_row) > 3 else []
            part2 = "@&@".join([clean_cell_value(val) for val in d_after_data])

        except Exception as e:
            logs.append(f"警告：读取示例数据失败 - {e}")

        full_str = part1 + part2
        dat_strings.append(full_str)
    
    logs.append(f"业务表单处理完成，共生成{len(dat_strings)}行数据")
    return dat_strings, business_en_name

# --- Streamlit 前端界面和主逻辑 ---

def main():
    st.set_page_config(page_title="Excel转Dat在线工具", layout="wide")
    
    st.title("Excel转Dat在线工具 (多sheet关联版)")
    st.markdown("""
    本工具可以将特定格式的Excel文件转换为Dat文件，并打包成ZIP。
    请上传包含「任务说明」、「业务说明」和「校验结果」sheet的Excel文件。
    """)

    # 文件上传组件
    uploaded_file = st.file_uploader("选择Excel文件", type=["xlsx", "xls"])

    if uploaded_file is not None:
        st.success(f"已成功上传文件：{uploaded_file.name}")
        
        # 开始处理按钮
        if st.button("开始处理"):
            # 创建一个占位符用于显示实时日志
            log_placeholder = st.empty()
            logs = []
            
            try:
                # 读取Excel文件
                excel_file = pd.ExcelFile(uploaded_file)
                sheet_names = excel_file.sheet_names
                logs.append(f"成功读取Excel文件，包含以下sheet: {', '.join(sheet_names)}")

                # 检查必需sheet
                required_sheets = ["任务说明", "业务说明"]
                missing_required_sheets = [s for s in required_sheets if s not in sheet_names]
                if missing_required_sheets:
                    raise ValueError(f"缺少必需的sheet: {', '.join(missing_required_sheets)}")
                
                # 读取任务说明
                df_task = pd.read_excel(excel_file, sheet_name="任务说明", header=1, dtype=str)
                credit_code, data_date = read_task_info(df_task)
                logs.append(f"社会统一信用代码：{credit_code}")
                logs.append(f"数据日期：{data_date}")

                # 读取业务说明
                df_business = pd.read_excel(excel_file, sheet_name="业务说明", header=1, dtype=str)
                business_list = read_business_info(df_business)
                if not business_list:
                    business_list = [{"业务表单名称": "默认表单", "业务表单英文名称": "Default"}]
                logs.append(f"共获取{len(business_list)}个业务表单配置")

                # 读取校验结果
                has_check_sheet = "校验结果" in sheet_names
                check_group = None
                if has_check_sheet:
                    df_check = pd.read_excel(excel_file, sheet_name="校验结果", header=1, dtype=str)
                    if "表单名称" in df_check.columns:
                        check_group = df_check.groupby("表单名称", dropna=False) if not df_check.empty else None
                        logs.append(f"「校验结果」sheet共{len(df_check)}行数据，已按表单名称分组")
                    else:
                        logs.append("警告：「校验结果」sheet缺少「表单名称」列")

                # 处理所有业务表单
                all_zip_bytes = []
                for business in business_list:
                    dat_strings, business_en_name = process_business_data(
                        excel_file, business, check_group, credit_code, data_date, logs
                    )
                    
                    # 生成DAT文件内容
                    dat_content = "\n".join(dat_strings)
                    dat_bytes = dat_content.encode("utf-8")
                    
                    # 生成LOG文件内容
                    base_filename = f"{credit_code}_{business_en_name}_CHK_{data_date}"
                    dat_filename = f"{base_filename}.dat"
                    
                    md5_value = get_bytes_md5(dat_bytes)
                    file_size = len(dat_bytes)
                    line_count = len([s for s in dat_strings if s.strip()])
                    create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    log_content = "\n".join([
                        dat_filename,
                        md5_value,
                        str(file_size),
                        create_time,
                        str(line_count)
                    ])
                    log_bytes = log_content.encode("utf-8")
                    
                    # 生成单个ZIP文件
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                        zipf.writestr(dat_filename, dat_bytes)
                        zipf.writestr(f"{base_filename}.log", log_bytes)
                    
                    zip_buffer.seek(0)
                    all_zip_bytes.append((f"{base_filename}.zip", zip_buffer.getvalue()))
                    
                    logs.append(f"已生成ZIP文件：{base_filename}.zip")

                # 生成总ZIP文件
                total_zip_buffer = io.BytesIO()
                with zipfile.ZipFile(total_zip_buffer, "w", zipfile.ZIP_DEFLATED) as total_zipf:
                    for zip_name, zip_data in all_zip_bytes:
                        total_zipf.writestr(zip_name, zip_data)
                
                total_zip_buffer.seek(0)
                logs.append(f"\n✅ 所有文件处理完成！共生成{len(all_zip_bytes)}个ZIP包")
                
                # 显示日志和下载按钮
                log_placeholder.markdown("### 处理日志\n" + "\n".join([f"> {log}" for log in logs]))
                st.download_button(
                    label="下载汇总ZIP文件",
                    data=total_zip_buffer,
                    file_name=f"汇总校验结果_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.zip",
                    mime="application/zip"
                )

            except Exception as e:
                logs.append(f"\n❌ 处理过程中发生错误: {str(e)}")
                log_placeholder.markdown("### 处理日志\n" + "\n".join([f"> {log}" for log in logs]))
                st.error(f"处理失败: {str(e)}")

if __name__ == "__main__":
    main()
