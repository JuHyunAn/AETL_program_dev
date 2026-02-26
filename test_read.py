import pandas as pd

excel_file = 'documents/IM_DW_D_09.매핑정의서_TB_DF_BC_BRC_PGM_CST_ORD_20170824.xlsx'
with open('output.txt', 'w', encoding='utf-8') as f:
    try:
        xl = pd.ExcelFile(excel_file)
        f.write('Sheets: ' + str(xl.sheet_names) + '\n')
        for sheet in xl.sheet_names:
            df = xl.parse(sheet, dtype=str)
            f.write(f'\n--- Sheet: {sheet} ---\n')
            f.write('Columns: ' + str(list(df.columns)) + '\n')
            f.write('First 5 rows:\n')
            f.write(df.head(5).to_markdown() + '\n')
    except Exception as e:
        f.write('Error: ' + str(e))
