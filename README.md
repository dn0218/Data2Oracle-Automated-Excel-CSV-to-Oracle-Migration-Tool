# Data2Oracle: Automated Excel/CSV to Oracle Migration Tool 
### Data2Oracle: Excel/CSV 自动化导入 Oracle 工具

This is a robust Python-based utility designed to streamline the process of migrating data from Excel (`.xlsx`, `.xls`) and CSV files into an Oracle Database. It handles name sanitization, Oracle keyword conflicts, and supports both standard SQL and high-speed **SQL*Loader**.

这是一个基于 Python 的自动化工具，旨在简化将 Excel (`.xlsx`, `.xls`) 和 CSV 文件数据迁移到 Oracle 数据库的过程。它支持名称自动清洗、Oracle 关键字冲突处理，并提供标准 SQL 插入及 **SQL*Loader** 高速导入两种模式。

---

## ✨ Features / 主要功能

* **Smart Sanitization / 智能名称处理**: 
    * Automatically cleans table/column names to comply with Oracle conventions (removes special characters, handles lengths). 
    * 自动处理表名和列名，确保符合 Oracle 命名规范（去除特殊字符、处理长度限制）。
* **Keyword Protection / 关键字保护**: 
    * Prefixes Oracle reserved words (e.g., `SELECT`, `DATE`, `ORDER`) with `COL_` to prevent syntax errors.
    * 自动为 Oracle 保留关键字（如 `SELECT`, `DATE`）添加 `COL_` 前缀，防止 SQL 执行报错。
* **Multi-Sheet Support / 多 Sheet 支持**: 
    * Detects multiple sheets in Excel and allows user selection via a menu.
    * 自动识别 Excel 中的多个 Sheet 页并允许用户通过菜单手动选择。
* **High Performance / 高性能导入**: 
    * Supports **SQL*Loader (sqlldr)** for processing large datasets efficiently.
    * 支持调用 Oracle 原生工具 **SQL*Loader (sqlldr)**，实现百万级数据的高速导入。
* **Safe Execution / 安全执行**: 
    * Pre-checks for existing tables with an interactive option to drop/recreate.
    * 自动检查目标表是否存在，并提供交互式的删除/重建选项。

---

## 🛠️ Prerequisites / 环境准备

### 1. Oracle Instant Client
The script requires the Oracle Instant Client binaries. Please ensure the path in the script matches your local installation.
脚本依赖 Oracle Instant Client。请确保脚本顶部的路径与你本地安装路径一致。
* **Default Path in Script / 脚本内默认路径**: `C:\oracle\instantclient...`

### 2. Python Dependencies / Python 依赖库
```bash
pip install pandas oracledb openpyxl
```
**### 3. Oracle Binaries / 系统环境**
To use SQL*Loader (Mode 5/6), the sqlldr command must be installed and accessible in your system.
如需使用 SQL*Loader (模式 5/6)，请确保系统已安装并配置好 sqlldr 命令行工具。

⚙️ Configuration / 配置文件说明
Create a db.ini file in the project root directory / 在项目根目录下创建 db.ini 文件

🚀 How to Use / 操作指南
Run the script using Python / 运行脚本

**Recommendation**
- For small amount data (10000 rows<), use the flow 1,2,3,4. 数据量小时，执行1，2，3，4选项。
- For large amount data, use the flow 1,2,5,6. 数据量大时，执行1，2，5，6选项。

Menu Options / 菜单说明:
1. Select File / 选择文件: Load .xlsx or .csv and parse structure. / 加载并解析文件结构。

2. Generate Create SQL / 生成建表 SQL: Create CREATE TABLE script in SqlFile/. / 在 SqlFile/ 目录下生成建表语句。

3. Generate Insert SQL / 生成插入 SQL: Create standard INSERT statements. / 生成标准 INSERT 插入脚本。

4. Execute SQL Files / 执行 SQL 文件: Automatically connect to DB and run the scripts. / 自动连接数据库并按序执行生成的 SQL。

5. Generate SQLLoader Script / 生成 SQLLoader 脚本: Prepare .ctl and .txt data files. / 准备控制文件与脱敏后的数据文本。

6. Full SQLLoader Import / 一键 SQLLoader 导入: Automate Table Check -> Create -> Load. / 自动完成全流程：查表 -> 建表 -> 调用 sqlldr 导入。

📂 Project Structure / 目录结构
```bash
.
├── main.py              # Main execution logic / 主程序逻辑
├── db.ini               # DB credentials (Manual) / 数据库配置 (手动创建)
└── SqlFile/             # Auto-generated assets / 自动生成的资源目录
    ├── XXX_create.sql   # Table definition / 建表语句
    ├── XXX.ctl          # SQL*Loader Control file / 控制文件
    ├── XXX_data.txt     # Cleaned data for sqlldr / 处理后的数据文本
    ├── XXX.log          # sqlldr execution log / 执行日志
    └── XXX.bad          # Failed records / 导入失败的数据记录
```

📝 License / 许可说明
This project is open-source. Feel free to modify it to fit your specific workflow needs.
本项目开源。您可以根据具体的业务需求自由修改和使用。
