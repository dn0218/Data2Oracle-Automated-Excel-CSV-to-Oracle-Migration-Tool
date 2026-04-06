import os
import re
import pandas as pd
import configparser
import oracledb as cx_Oracle
import time

# 设置 Oracle Instant Client 路径
oracle_bin = r"C:\oracle\instantclient-basic-windows.x64-23.9.0.25.07\instantclient_23_9"
if oracle_bin not in os.environ["PATH"]:
    os.environ["PATH"] = oracle_bin + ";" + os.environ["PATH"]

os.system("where sqlldr")


# 在文件顶部添加Oracle关键字列表（在所有import语句之后）
ORACLE_KEYWORDS = {
    'ACCESS', 'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'AUDIT', 'BETWEEN', 'BY', 'CHAR', 'CHECK', 
    'CLUSTER', 'COLUMN', 'COMMENT', 'COMPRESS', 'CONNECT', 'CREATE', 'CURRENT', 'DATE', 'DECIMAL', 'DEFAULT', 
    'DELETE', 'DESC', 'DISTINCT', 'DROP', 'ELSE', 'EXCLUSIVE', 'EXISTS', 'FILE', 'FLOAT', 'FOR', 'FROM', 
    'GRANT', 'GROUP', 'HAVING', 'IDENTIFIED', 'IMMEDIATE', 'IN', 'INCREMENT', 'INDEX', 'INITIAL', 'INSERT', 
    'INTEGER', 'INTERSECT', 'INTO', 'IS', 'LEVEL', 'LIKE', 'LOCK', 'LONG', 'MAXEXTENTS', 'MINUS', 'MLSLABEL', 
    'MODE', 'MODIFY', 'NOAUDIT', 'NOCOMPRESS', 'NOT', 'NOWAIT', 'NULL', 'NUMBER', 'OF', 'OFFLINE', 'ON', 
    'ONLINE', 'OPTION', 'OR', 'ORDER', 'PCTFREE', 'PRIOR', 'PRIVILEGES', 'PUBLIC', 'RAW', 'RENAME', 'RESOURCE', 
    'REVOKE', 'ROW', 'ROWID', 'ROWNUM', 'ROWS', 'SELECT', 'SESSION', 'SET', 'SHARE', 'SIZE', 'SMALLINT', 
    'START', 'SUCCESSFUL', 'SYNONYM', 'SYSDATE', 'TABLE', 'THEN', 'TO', 'TRIGGER', 'UID', 'UNION', 'UNIQUE', 
    'UPDATE', 'USER', 'VALIDATE', 'VALUES', 'VARCHAR', 'VARCHAR2', 'VIEW', 'WHENEVER', 'WHERE', 'WITH'
}

def sanitize_name(name, max_length=128, is_column=False):
    # 替换特殊字符为下划线，并确保符合Oracle命名规范
    sanitized = re.sub(r'[^a-zA-Z0-9_$#]', '_', name)
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')

    # 处理数字开头的情况和表名前缀
    if sanitized:
        if is_column:
            # 检查是否为Oracle关键字，如果是则添加 COL_ 前缀
            if sanitized.upper() in ORACLE_KEYWORDS:
                sanitized = 'COL_' + sanitized
            elif sanitized[0].isdigit():
                sanitized = 'COL_' + sanitized
        else:
            # 为表名添加tmp_前缀
            if not sanitized.startswith('TMP_'):
                sanitized = 'TMP_' + sanitized

    # 截断到最大长度
    return sanitized[:max_length].upper()


def get_table_name(file_path, sheet_name=None, is_multi_sheet=False):
    """
    根据文件路径和sheet名称生成表名
    is_multi_sheet: 是否是多sheet的情况
    """
    if is_multi_sheet and sheet_name:
        # 多sheet情况下使用sheet名作为表名
        return sanitize_name(sheet_name)
    else:
        # 单sheet或未指定sheet时使用文件名
        base_name = os.path.basename(file_path)
        file_name, _ = os.path.splitext(base_name)
        return sanitize_name(file_name)


def process_columns(df):
    return [sanitize_name(col, is_column=True) for col in df.columns]


def generate_create_sql(table_name, columns):
    columns_sql_parts = []
    for col in columns:
        if col.upper() == 'SALES_DESCRIPTION':
            columns_sql_parts.append(f'    {col} VARCHAR2(2000)')
        else:
            columns_sql_parts.append(f'    {col} VARCHAR2(400)')
    columns_sql = ',\n'.join(columns_sql_parts)
    return f"CREATE TABLE {table_name} (\n{columns_sql}\n);\n"


def generate_insert_sql(table_name, columns, data_rows):
    sql = []
    for row in data_rows:
        values = []
        for value in row:
            if pd.isna(value):
                values.append("NULL")
            else:
                str_val = str(value)
                str_val = str_val.replace("'", "''")
                str_val = str_val.replace("&", "CHR(38)")
                # 先去除所有控制字符，再替换所有空白字符为单空格
                str_val = re.sub(r'[\x00-\x1F\x7F]', '', str_val)
                str_val = re.sub(r'\s+', ' ', str_val)
                str_val = str_val.strip()
                values.append(f"'{str_val}'")
        sql.append(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});")
    return '\n'.join(sql)


def generate_data_file(table_name, df, columns):
    # 将数据保存为txt文件，字段用逗号分隔
    output_file = f"{table_name}_data.txt"

    # 处理数据中的逗号
    df_copy = df.copy()
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].apply(lambda x: str(x).replace(',', '_') if pd.notna(x) else '')

    # 保存数据文件
    df_copy.to_csv(output_file, index=False, header=False, sep=',', encoding='utf-8')
    return output_file


def generate_ctl_file(table_name, columns):
    # 生成SQLLoader控制文件
    column_list = []
    for col in columns:
        column_list.append("    {}".format(col))

    # 获取数据文件的绝对路径
    sql_dir = ensure_sql_directory()
    data_file = os.path.abspath(os.path.join(sql_dir, f"{table_name}_data.txt"))

    columns_str = ',\n'.join(column_list)
    ctl_content = f"""LOAD DATA
INFILE '{data_file}'
INTO TABLE {table_name}
FIELDS TERMINATED BY ','
TRAILING NULLCOLS
(
{columns_str}
)"""

    ctl_file = os.path.join(sql_dir, f"{table_name}.ctl")
    with open(ctl_file, 'w', encoding='utf-8') as f:
        f.write(ctl_content)
    return ctl_file


def get_db_config():
    config = configparser.ConfigParser()
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # 在脚本所在目录查找db.ini
    ini_path = os.path.join(script_dir, 'db.ini')
    if not os.path.exists(ini_path):
        raise FileNotFoundError(f"db.ini configuration file not found at: {ini_path}")
    config.read(ini_path)
    return config['db_config']


def ensure_sql_directory():
    """确保SQL文件输出目录存在"""
    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'SqlFile')
    if not os.path.exists(sql_dir):
        os.makedirs(sql_dir)
    return sql_dir


def get_output_path(base_name):
    """获取输出文件的完整路径"""
    sql_dir = ensure_sql_directory()
    return os.path.join(sql_dir, base_name)


def execute_sql_file(cursor, sql_file):
    """执行SQL文件"""
    print(f"正在执行SQL文件: {sql_file}")
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
        
    # 分割SQL语句（按分号分割）
    sql_statements = sql_content.split(';')
    
    for statement in sql_statements:
        statement = statement.strip()
        if statement:  # 忽略空语句
            try:
                cursor.execute(statement)
            except Exception as e:
                print(f"执行SQL语句时出错: {str(e)}")
                print(f"问题语句: {statement}")
                raise


def check_table_exists(cursor, table_name):
    """检查表是否存在"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM user_tables WHERE table_name = '{table_name}'")
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        print(f"检查表是否存在时出错: {str(e)}")
        raise


def drop_table(cursor, table_name):
    """删除表"""
    try:
        cursor.execute(f"DROP TABLE {table_name}")
        print(f"表 {table_name} 已成功删除")
    except Exception as e:
        print(f"删除表时出错: {str(e)}")
        raise


def execute_sql_files(create_file, insert_file):
    """执行建表和插入数据的SQL文件"""
    try:
        # 获取数据库配置
        db_config = get_db_config()
        
        # 构建连接字符串
        dsn = cx_Oracle.makedsn(
            host=db_config['host'],
            port=int(db_config['port']),
            service_name=db_config['service_name']
        )
        
        # 连接数据库
        print("正在连接数据库...")
        connection = cx_Oracle.connect(
            user=db_config['username'],
            password=db_config['password'],
            dsn=dsn
        )
        
        try:
            cursor = connection.cursor()
            
            # 从建表SQL文件中提取表名
            with open(create_file, 'r', encoding='utf-8') as f:
                create_sql = f.read()
                # 使用正则表达式提取表名
                table_name_match = re.search(r'CREATE TABLE (\w+)', create_sql)
                if not table_name_match:
                    raise ValueError("无法从建表SQL中提取表名")
                table_name = table_name_match.group(1)
            
            # 检查表是否存在
            if check_table_exists(cursor, table_name):
                print(f"\n表 {table_name} 已存在")
                response = input("是否要删除已存在的表？(y/n): ").strip().lower()
                if response == 'y':
                    drop_table(cursor, table_name)
                    connection.commit()
                else:
                    print("操作已取消")
                    return
            
            # 执行建表SQL
            print("\n开始执行建表SQL...")
            execute_sql_file(cursor, create_file)
            print("建表SQL执行完成")
            
            # 提交建表操作
            connection.commit()
            
            # 执行插入数据SQL
            print("\n开始执行插入数据SQL...")
            execute_sql_file(cursor, insert_file)
            print("插入数据SQL执行完成")
            
            # 提交插入操作
            connection.commit()
            
            print("\n所有SQL文件执行完成！")
            
        finally:
            cursor.close()
            connection.close()
            print("数据库连接已关闭")
            
    except Exception as e:
        print(f"执行SQL文件时出错: {str(e)}")
        raise


def main():
    file_path = None
    table_name = None
    df = None
    sanitized_columns = None
    selected_sheet = None
    create_file = None
    insert_file = None

    # 确保SQL输出目录存在
    sql_dir = ensure_sql_directory()
    print(f"SQL文件将输出到目录: {sql_dir}")

    while True:
        mode = input(
            "\n请选择操作模式：\n0. 退出程序\n1. 选择文件\n2. 生成建表SQL\n3. 生成插入数据SQL\n4. 执行SQL文件\n5. 生成SQLLoader脚本\n6. 执行SQLLoader脚本（自动创建表）\n请输入数字选择：").strip()

        if mode == '0':
            print("程序已退出")
            return
        elif mode == '1':
            file_path = input("请输入文件路径：").strip()
            if not os.path.exists(file_path):
                print("文件不存在！")
                continue

            try:
                if file_path.lower().endswith(('.xls', '.xlsx')):
                    # 读取所有sheet页信息
                    excel_file = pd.ExcelFile(file_path)
                    sheet_names = excel_file.sheet_names
                    
                    if len(sheet_names) > 1:
                        print("\n检测到多个sheet页，请选择要导入的sheet页：")
                        for idx, name in enumerate(sheet_names):
                            print(f"{idx}. {name}")
                        
                        while True:
                            try:
                                sheet_idx = int(input("\n请输入sheet页索引号："))
                                if 0 <= sheet_idx < len(sheet_names):
                                    selected_sheet = sheet_names[sheet_idx]
                                    break
                                else:
                                    print("无效的索引号，请重新输入！")
                            except ValueError:
                                print("请输入有效的数字！")
                        
                        # 读取选定的sheet页
                        df = pd.read_excel(file_path, sheet_name=selected_sheet, header=0, dtype=str)
                        # 使用sheet名作为表名
                        table_name = get_table_name(file_path, selected_sheet, True)
                    else:
                        # 单sheet的情况
                        df = pd.read_excel(file_path, sheet_name=0, header=0, dtype=str)
                        selected_sheet = sheet_names[0]
                        # 使用文件名作为表名
                        table_name = get_table_name(file_path)
                        
                elif file_path.lower().endswith('.csv'):
                    df = pd.read_csv(file_path, header=0, dtype=str, encoding='utf-8')
                    table_name = get_table_name(file_path)
                else:
                    print("不支持的文件类型，请使用Excel或CSV文件")
                    continue
                    
                sanitized_columns = process_columns(df)
                print(f"\n文件已成功加载")
                print(f"表名：{table_name}")
                if selected_sheet:
                    print(f"选择的sheet页：{selected_sheet}")
                print(f"列数：{len(sanitized_columns)}")
                print(f"行数：{len(df)}")
                
            except Exception as e:
                print(f"读取文件失败：{str(e)}")
                continue

        elif mode == '2':
            if file_path is None:
                print("请先选择文件（选项1）")
                continue

            # 生成建表脚本
            create_sql = generate_create_sql(table_name, sanitized_columns)
            create_file = get_output_path(f"{table_name}_create.sql")
            with open(create_file, 'w', encoding='utf-8') as f:
                f.write(create_sql)
            print(f"已生成建表脚本：{create_file}")
        elif mode == '3':
            if file_path is None:
                print("请先选择文件（选项1）")
                continue

            # 生成插入脚本
            data_rows = [row.tolist() for _, row in df.iterrows()]
            insert_sql = generate_insert_sql(table_name, sanitized_columns, data_rows)
            insert_file = get_output_path(f"{table_name}_insert.sql")
            with open(insert_file, 'w', encoding='utf-8') as f:
                f.write(insert_sql)
            print(f"已生成插入脚本：{insert_file}")
        elif mode == '4':
            if not create_file or not insert_file:
                print("请先生成建表SQL和插入数据SQL（选项2和3）")
                continue
                
            try:
                execute_sql_files(create_file, insert_file)
            except Exception as e:
                print(f"执行SQL文件失败：{str(e)}")
                continue
        elif mode == '5':
            if file_path is None:
                print("请先选择文件（选项1）")
                continue
            try:
                # 获取数据库配置
                db_config = get_db_config()

                # 生成数据文件
                data_file = get_output_path(f"{table_name}_data.txt")
                # 处理数据中的逗号
                df_copy = df.copy()
                for col in df_copy.columns:
                    df_copy[col] = df_copy[col].apply(lambda x: str(x).replace(',', '_') if pd.notna(x) else '')
                # 保存数据文件
                df_copy.to_csv(data_file, index=False, header=False, sep=',', encoding='utf-8')

                # 生成控制文件（用绝对路径）
                ctl_file = generate_ctl_file(table_name, sanitized_columns)

                # 生成日志文件路径
                log_file = get_output_path(f"{table_name}.log")
                bad_file = get_output_path(f"{table_name}.bad")

                # 生成SQLLoader命令
                sqlldr_cmd = (f"sqlldr {db_config['username']}/{db_config['password']}@"
                            f"{db_config['host']}:{db_config['port']}/{db_config['service_name']} "
                            f"control={ctl_file} rows=10000 log={log_file} bad={bad_file}")

                print(f"已生成数据文件：{data_file}")
                print(f"已生成控制文件：{ctl_file}")
                print("\nSQLLoader导入命令：")
                print(sqlldr_cmd)

            except Exception as e:
                print(f"生成SQLLoader脚本失败：{str(e)}")
                continue
        elif mode == '6':
            if file_path is None:
                print("请先选择文件（选项1）并生成SQLLoader脚本（选项5）")
                continue
            try:
                db_config = get_db_config()
                sql_dir = ensure_sql_directory()
                data_file = get_output_path(f"{table_name}_data.txt")
                ctl_file = get_output_path(f"{table_name}.ctl")
                log_file = get_output_path(f"{table_name}.log")
                bad_file = get_output_path(f"{table_name}.bad")
                
                # 检查文件是否存在
                if not (os.path.exists(data_file) and os.path.exists(ctl_file)):
                    print("请先生成SQLLoader脚本（选项5），确保数据文件和ctl文件已生成！")
                    continue
                
                # 自动创建表（如果还没有创建的话）
                create_file = get_output_path(f"{table_name}_create.sql")
                if os.path.exists(create_file):
                    print(f"\n检测到建表SQL文件，正在自动创建表 {table_name}...")
                    try:
                        # 构建连接字符串
                        dsn = cx_Oracle.makedsn(
                            host=db_config['host'],
                            port=int(db_config['port']),
                            service_name=db_config['service_name']
                        )
                        
                        # 连接数据库
                        connection = cx_Oracle.connect(
                            user=db_config['username'],
                            password=db_config['password'],
                            dsn=dsn
                        )
                        
                        try:
                            cursor = connection.cursor()
                            
                            # 检查表是否存在
                            if check_table_exists(cursor, table_name):
                                print(f"表 {table_name} 已存在")
                                response = input("是否要删除已存在的表？(y/n): ").strip().lower()
                                if response == 'y':
                                    drop_table(cursor, table_name)
                                    connection.commit()
                                else:
                                    print("操作已取消")
                                    return
                            
                            # 执行建表SQL
                            print("开始执行建表SQL...")
                            execute_sql_file(cursor, create_file)
                            print("建表SQL执行完成")
                            connection.commit()
                            
                        finally:
                            cursor.close()
                            connection.close()
                            print("数据库连接已关闭")
                            
                    except Exception as e:
                        print(f"自动创建表失败：{str(e)}")
                        print("请手动执行选项4来创建表，或检查建表SQL文件是否正确")
                        continue
                else:
                    print(f"未找到建表SQL文件：{create_file}")
                    print("请先生成建表SQL（选项2）")
                    continue
                
                # 执行SQLLoader命令
                sqlldr_cmd = (f"sqlldr {db_config['username']}/{db_config['password']}@"
                              f"{db_config['host']}:{db_config['port']}/{db_config['service_name']} "
                              f"control=\"{ctl_file}\" rows=10000 log=\"{log_file}\" bad=\"{bad_file}\"")
                print("\n即将执行SQLLoader命令：")
                print(sqlldr_cmd)
                confirm = input("是否继续执行？(y/n): ").strip().lower()
                if confirm != 'y':
                    print("操作已取消")
                    continue
                # 执行命令
                exit_code = os.system(sqlldr_cmd)
                if exit_code == 0:
                    print(f"SQLLoader执行完成，日志文件：{log_file}")
                else:
                    print(f"SQLLoader执行失败，退出码：{exit_code}，请检查日志文件：{log_file}")
            except Exception as e:
                print(f"执行SQLLoader脚本失败：{str(e)}")
                continue
        else:
            print("无效的输入，请输入0、1、2、3、4、5或6")


if __name__ == "__main__":
    main()
