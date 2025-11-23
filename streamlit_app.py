import streamlit as st
import pandas as pd

from netkeiba_scraper import get_shutuba_by_date, export_one_book_all_venues_pretty_to_bytes, normalize_ymd

st.set_page_config(page_title="netkeiba 出馬表生成", layout="wide")

st.title("netkeiba 出馬表生成ツール")

ymd_input = st.text_input("取得日 (YYYYMMDD または YYMMDD)", value="")

if st.button("出馬表を取得してExcelを生成"):
    try:
        ymd = normalize_ymd(ymd_input)
    except ValueError as e:
        st.error(str(e))
    else:
        with st.spinner("出馬表を取得中..."):
            df = get_shutuba_by_date(ymd)
        if df.empty:
            st.warning("対象日のレースが見つかりませんでした。開催日かどうか確認してください。")
        else:
            st.success(f"取得行数: {len(df)} 行")
            excel_bytes = export_one_book_all_venues_pretty_to_bytes(df)
            filename = f"出馬表_{ymd}.xlsx"
            st.download_button(
                label="出馬表Excelをダウンロード",
                data=excel_bytes,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
