import re
import unicodedata
from io import BytesIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
BASE = "https://race.netkeiba.com"


def _to_half(s: str) -> str:
    if s is None:
        return ""
    return unicodedata.normalize("NFKC", s).strip()


def normalize_ymd(ymd: str) -> str:
    s = ymd.strip()
    if len(s) == 6 and s.isdigit():
        return "20" + s
    if len(s) == 8 and s.isdigit():
        return s
    raise ValueError("日付は YYYYMMDD または YYMMDD の数字で入力してください")


def get_race_ids_requests(yyyymmdd: str, timeout: int = 15) -> list[str]:
    ymd = yyyymmdd
    race_ids: set[str] = set()

    for place in range(1, 11):
        url = f"{BASE}/top/race_list_sub.html?kaisai_date={ymd}&kaisai_place={place:02d}"
        resp = requests.get(url, headers=HEADERS, timeout=timeout)

        text = None
        for enc in [resp.apparent_encoding, "utf-8", "euc_jp", "shift_jis", "cp932"]:
            try:
                resp.encoding = enc
                t = resp.text
                if "RaceList_DataItem" in t or "race_id=" in t:
                    text = t
                    break
            except Exception:
                pass
        if text is None:
            resp.encoding = resp.apparent_encoding or "utf-8"
            text = resp.text

        soup = BeautifulSoup(text, "lxml")

        for a in soup.select(".RaceList_DataItem a[href*='race_id=']"):
            href = a.get("href") or ""
            m = re.search(r"race_id=(\d{12})", href)
            if m:
                race_ids.add(m.group(1))

    return sorted(race_ids)


def fetch_shutuba_df(race_id: str) -> pd.DataFrame:
    url = f"{BASE}/race/shutuba.html?race_id={race_id}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    text = None
    for enc in [resp.apparent_encoding, "utf-8", "euc_jp", "shift_jis", "cp932"]:
        try:
            resp.encoding = enc
            t = resp.text
            if re.search(r"出馬表|馬番|騎手", t):
                text = t
                break
        except Exception:
            pass
    if text is None:
        resp.encoding = resp.apparent_encoding or "utf-8"
        text = resp.text
    soup = BeautifulSoup(text, "lxml")
    title = soup.title.get_text(strip=True) if soup.title else ""
    race_loc, race_no = "", ""
    m = re.search(r"(\S+?)(\d+)R", title)
    if m:
        race_loc = m.group(1)
        race_no = f"{m.group(2)}R"
    post_time = ""
    block = soup.select_one(".RaceData01")
    if block:
        text_all = " ".join(block.stripped_strings)
        mm = re.search(r"(\d{1,2}:\d{2})", text_all)
        if mm:
            post_time = mm.group(1)

    def find_main_table(sp):
        for tbl in sp.find_all("table"):
            head_txt = " ".join(h.get_text(strip=True) for h in tbl.find_all(["th", "td"])[:24])
            if re.search(r"枠|馬番|馬名|騎手", head_txt):
                return tbl
        return None

    tbl = find_main_table(soup)
    if not tbl:
        return pd.DataFrame(columns=["競馬場名", "レース", "発走時刻", "枠番", "馬番", "馬名", "騎手名"])
    header = tbl.find("tr")
    headers = [h.get_text(strip=True) for h in header.find_all(["th", "td"])] if header else []
    idx = {"waku": None, "umaban": None, "bamei": None, "kisyumei": None}
    for i, h in enumerate(headers):
        if idx["waku"] is None and "枠" in h:
            idx["waku"] = i
        if idx["umaban"] is None and "馬番" in h:
            idx["umaban"] = i
        if idx["bamei"] is None and "馬名" in h:
            idx["bamei"] = i
        if idx["kisyumei"] is None and "騎手" in h:
            idx["kisyumei"] = i
    rows = []
    seen_umaban = set()
    for tr in tbl.find_all("tr")[1:]:
        if tr.find("th"):
            continue
        tds = tr.find_all("td")
        if not tds:
            continue
        cols = [c.get_text(strip=True) for c in tds]

        def pick(name):
            j = idx[name]
            return cols[j].strip() if (j is not None and j < len(cols)) else ""

        waku = _to_half(pick("waku"))
        umaban = _to_half(pick("umaban"))
        bamei = _to_half(pick("bamei"))
        kisy = _to_half(pick("kisyumei"))
        if not re.fullmatch(r"\d{1,2}", umaban or ""):
            continue
        if umaban in seen_umaban:
            continue
        seen_umaban.add(umaban)
        rows.append(
            {
                "競馬場名": race_loc,
                "レース": race_no,
                "発走時刻": post_time,
                "枠番": waku,
                "馬番": umaban,
                "馬名": bamei,
                "騎手名": kisy,
            }
        )
    return pd.DataFrame(rows, columns=["競馬場名", "レース", "発走時刻", "枠番", "馬番", "馬名", "騎手名"])


def get_shutuba_by_date(yyyymmdd: str) -> pd.DataFrame:
    ymd = normalize_ymd(yyyymmdd)
    race_ids = get_race_ids_requests(ymd)
    if not race_ids:
        return pd.DataFrame(columns=["競馬場名", "レース", "発走時刻", "枠番", "馬番", "馬名", "騎手名"])
    dfs = []
    for rid in race_ids:
        try:
            df_one = fetch_shutuba_df(rid)
            if not df_one.empty:
                dfs.append(df_one)
        except Exception as e:
            print(f"[WARN] {rid}: {e}")
    if not dfs:
        return pd.DataFrame(columns=["競馬場名", "レース", "発走時刻", "枠番", "馬番", "馬名", "騎手名"])
    df = pd.concat(dfs, ignore_index=True)
    df = (
        df.sort_values(["競馬場名", "レース", "馬番"])
        .drop_duplicates(subset=["競馬場名", "レース", "馬番"], keep="first")
        .reset_index(drop=True)
    )
    return df


def export_one_book_all_venues_pretty_to_bytes(df: pd.DataFrame, zoom: int = 165) -> bytes:
    cols = df.columns
    if {"競馬場名", "レース", "枠番", "馬番", "馬名", "騎手名"}.issubset(cols):
        df_norm = df[["競馬場名", "レース", "枠番", "馬番", "馬名", "騎手名"]].copy()
        if "発走時刻" in cols:
            df_norm["発走時刻"] = df["発走時刻"].astype(str)
        else:
            df_norm["発走時刻"] = pd.NA

        def _to_r_int(x):
            if pd.isna(x):
                return None
            m = re.search(r"\d+", str(x))
            return int(m.group()) if m else None

        df_norm["venue"] = df_norm["競馬場名"].astype(str)
        df_norm["R"] = df_norm["レース"].map(_to_r_int)
        df_norm["wakuban"] = pd.to_numeric(df_norm["枠番"], errors="coerce")
        df_norm["umaban"] = pd.to_numeric(df_norm["馬番"], errors="coerce")
        df_norm["bamei"] = df_norm["馬名"].astype(str)
        df_norm["kishu"] = df_norm["騎手名"].astype(str)
    else:
        raise ValueError("必要な列が見つかりません。")
    df_norm = df_norm.dropna(subset=["venue", "R", "wakuban", "umaban"]).copy()
    df_norm["R"] = df_norm["R"].astype(int)
    t = pd.to_datetime(df_norm["発走時刻"], format="%H:%M", errors="coerce")
    df_norm["__sort_time__"] = t
    order_tbl = df_norm.groupby(["venue", "R"], as_index=False).agg(sort_time=("__sort_time__", "min"))
    order_tbl["sort_time"] = order_tbl["sort_time"].fillna(pd.Timestamp.max)
    order_tbl = order_tbl.sort_values(["sort_time", "venue", "R"]).reset_index(drop=True)
    race_order = list(order_tbl.itertuples(index=False, name=None))
    WAKU_COLOR = {
        1: "#ffffff",
        2: "#000000",
        3: "#ff0000",
        4: "#0000ff",
        5: "#ffff00",
        6: "#00ff00",
        7: "#ff8000",
        8: "#ff8080",
    }
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        wb = writer.book
        header_base = {
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#eeeeee",
            "font_size": 14,
        }
        cell_base = {
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "font_size": 12,
        }
        wrap_base = {
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "font_size": 12,
            "text_wrap": True,
        }
        title_fmt = wb.add_format(
            {
                "bold": True,
                "align": "center",
                "valign": "vcenter",
                "font_size": 14,
                "border": 1,
                "bg_color": "#fffbe6",
            }
        )
        fmt_cache: dict[tuple, object] = {}

        def fmt(kind, top=False, bottom=False, left=False, right=False, bg=None, font_color=None):
            key = (kind, top, bottom, left, right, bg, font_color)
            if key in fmt_cache:
                return fmt_cache[key]
            base = header_base if kind == "header" else (wrap_base if kind == "wrap" else cell_base)
            d = dict(base)
            d.update(
                {
                    "top": 2 if top else 1,
                    "bottom": 2 if bottom else 1,
                    "left": 2 if left else 1,
                    "right": 2 if right else 1,
                }
            )
            if kind == "waku":
                d["bg_color"] = bg or "#ffffff"
                if font_color:
                    d["font_color"] = font_color
            fmt_cache[key] = wb.add_format(d)
            return fmt_cache[key]

        for venue, R, _ in race_order:
            g = (
                df_norm[(df_norm["venue"] == venue) & (df_norm["R"] == R)]
                .sort_values(["wakuban", "umaban"])
                .reset_index(drop=True)
            )
            sheet = f"{venue}-{int(R)}R"[:31]
            out = g[["wakuban", "umaban", "bamei", "kishu"]].copy()
            out.insert(2, "印", "")
            out.columns = ["枠番", "馬番", "印", "馬名", "騎手名"]
            out["コメント"] = ""
            out.to_excel(writer, sheet_name=sheet, index=False, startrow=1)
            ws = writer.sheets[sheet]
            ws.set_zoom(zoom)
            ws.hide_gridlines(2)
            ws.set_margins(0.3, 0.3, 0.3, 0.3)
            ws.set_default_row(22)
            ws.merge_range(0, 0, 0, 5, f"{venue}{int(R)}R　レースコメント：", title_fmt)
            ws.set_row(0, 42)
            ws.set_column(0, 0, 4)
            ws.set_column(1, 1, 4)
            ws.set_column(2, 2, 4)
            ws.set_column(3, 3, 20)
            ws.set_column(4, 4, 12)
            ws.set_column(5, 5, 42)
            HEADER_ROW = 1
            DATA_START = 2
            n = len(out)
            rightmost_col = 5
            bottom_row = DATA_START + n - 1
            for c, name in enumerate(["枠番", "馬番", "印", "馬名", "騎手名", "コメント"]):
                ws.write(
                    HEADER_ROW,
                    c,
                    name,
                    fmt("header", top=True, left=(c == 0), right=(c == rightmost_col)),
                )
            for r in range(n):
                wr = DATA_START + r
                ws.write(wr, 2, "", fmt("cell", top=(wr == DATA_START), bottom=(wr == bottom_row)))
                ws.write(
                    wr,
                    3,
                    str(out.iloc[r, 3]) if pd.notna(out.iloc[r, 3]) else "",
                    fmt("cell", top=(wr == DATA_START), bottom=(wr == bottom_row)),
                )
                ws.write(
                    wr,
                    4,
                    str(out.iloc[r, 4]) if pd.notna(out.iloc[r, 4]) else "",
                    fmt("cell", top=(wr == DATA_START), bottom=(wr == bottom_row)),
                )
                ws.write(
                    wr,
                    5,
                    "",
                    fmt("wrap", top=(wr == DATA_START), bottom=(wr == bottom_row), right=True),
                )
            start = 0
            while start < n:
                w = out.iloc[start, 0]
                if pd.isna(w):
                    start += 1
                    continue
                end = start
                while end + 1 < n and out.iloc[end + 1, 0] == w:
                    end += 1
                w_int = int(w)
                bg = WAKU_COLOR.get(w_int, "#ffffff")
                font_color = "black" if w_int in (1, 5, 6) else "white"
                r1 = DATA_START + start
                r2 = DATA_START + end
                fmt_a = fmt(
                    "waku",
                    top=(r1 == DATA_START),
                    bottom=(r2 == bottom_row),
                    left=True,
                    right=False,
                    bg=bg,
                    font_color=font_color,
                )
                if r2 > r1:
                    ws.merge_range(r1, 0, r2, 0, w_int, fmt_a)
                else:
                    ws.write(r1, 0, w_int, fmt_a)
                for rr in range(r1, r2 + 1):
                    fmt_b = fmt(
                        "waku",
                        top=(rr == DATA_START),
                        bottom=(rr == bottom_row),
                        left=False,
                        right=False,
                        bg=bg,
                        font_color=font_color,
                    )
                    ws.write(rr, 1, int(out.iloc[rr - DATA_START, 1]), fmt_b)
                start = end + 1
            ws.data_validation(
                DATA_START,
                2,
                DATA_START + n - 1,
                2,
                {"validate": "list", "source": ["◎", "◯", "▲", "△"], "ignore_blank": True, "show_error": False},
            )
    output.seek(0)
    return output.getvalue()
