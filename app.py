import datetime
import os
import re
import sqlite3
import time

import duckdb
import pandas as pd
import requests
import s3fs
import streamlit as st

from API_KEY import MY_EXCHANGE_API_KEY, MY_STEAM_API_KEY

# --- KONFIGURASI API ---
EXCHANGE_API_KEY = MY_EXCHANGE_API_KEY
STEAM_API_KEY = MY_STEAM_API_KEY

# --- KONFIGURASI UI ---
st.set_page_config(page_title="Smart Game Planner", layout="wide")

MINIO_CONF = {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"},
}


# --- FUNGSI FORMATTER ---
def format_idr(val):
    return f"Rp {val:,.0f}".replace(",", ".")


# --- FUNGSI HELPER ---
def get_steam_library(steam_id):
    url = f"http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/"
    params = {
        "key": STEAM_API_KEY,
        "steamid": steam_id,
        "format": "json",
        "include_appinfo": True,
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        if "response" in data and "games" in data["response"]:
            return pd.DataFrame(
                [{"game_title": g["name"]} for g in data["response"]["games"]]
            )
        return pd.DataFrame(columns=["game_title"])
    except:
        return pd.DataFrame(columns=["game_title"])


# --- PIPELINE ELT ---
def run_pipeline(steam_id):
    status_box = st.status("🚀 Menjalankan Pipeline Analisis...", expanded=True)

    # 1. FINANCE
    status_box.write("📂 [1/5] Load Budget Data...")
    try:
        df_finance = pd.read_excel("finance_data.xlsx", engine="openpyxl")
        df_finance.to_csv(
            "s3://lakehouse/bronze/finance.csv", index=False, storage_options=MINIO_CONF
        )
    except:
        st.error("Gagal baca Excel Finance.")
        return None, 0, 0

    # 2. STEAM
    status_box.write("🎮 [2/5] Cek Inventory Steam...")
    df_steam = get_steam_library(steam_id)
    with sqlite3.connect("library.db") as con_sql:
        df_steam.to_sql("steam_inventory", con_sql, if_exists="replace", index=False)

    # 3. KURS
    status_box.write("💱 [3/5] Update Kurs Dollar...")
    try:
        # --- PERUBAHAN: Menggunakan Endpoint PAIR (Lebih Efisien) ---
        url_kurs = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_API_KEY}/pair/USD/IDR"
        res_kurs = requests.get(url_kurs).json()
        rate_idr = res_kurs["conversion_rate"]

        pd.DataFrame([{"rate": rate_idr}]).to_csv(
            "s3://lakehouse/bronze/kurs.csv", index=False, storage_options=MINIO_CONF
        )
    except Exception as e:
        print(f"Error Kurs: {e}")
        rate_idr = 16000.0

    # 4. MARKET (DEALS + LOWEST PRICE CHECK)
    status_box.write("🔥 [4/5] Cek Harga Pasar & Historical Low...")

    # A. Ambil Deals
    deals = requests.get(
        "https://www.cheapshark.com/api/1.0/deals?storeID=1&upperPrice=50&pageSize=30"
    ).json()
    df_market = pd.DataFrame(deals)

    # B. Cek Lowest Price via Game ID
    try:
        game_ids = df_market["gameID"].unique().tolist()
        ids_str = ",".join([str(id) for id in game_ids])

        games_url = f"https://www.cheapshark.com/api/1.0/games?ids={ids_str}"
        games_response = requests.get(games_url).json()

        cheapest_map = {}
        for gid, info in games_response.items():
            try:
                price = float(info.get("cheapestPriceEver", {}).get("price", 0))
                cheapest_map[str(gid)] = price
            except:
                cheapest_map[str(gid)] = 0.0

        df_market["cheapestPrice"] = df_market["gameID"].astype(str).map(cheapest_map)

    except Exception as e:
        st.warning(f"Gagal mengambil data Lowest Price: {e}")
        df_market["cheapestPrice"] = 0.0

    df_market.to_csv(
        "s3://lakehouse/bronze/market_data.csv", index=False, storage_options=MINIO_CONF
    )

    # 5. ADVANCED ANALYTICS (DUCKDB)
    status_box.write("🧠 [5/5] Finalisasi Keputusan (AI Scoring)...")

    current_month_id = int(datetime.datetime.now().strftime("%Y%m"))

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(
        f"SET s3_endpoint='localhost:9000'; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'; SET s3_use_ssl=false; SET s3_url_style='path';"
    )

    # --- PERBAIKAN QUERY (Menggunakan CTE 'final_calc') ---
    query = f"""
    WITH
    rate AS (SELECT rate FROM read_csv('s3://lakehouse/bronze/kurs.csv', header=True) LIMIT 1),

    fin AS (
        SELECT SUM(budget_final_game) as total
        FROM read_csv('s3://lakehouse/bronze/finance.csv', header=True)
        WHERE bulan_id <= {current_month_id}
    ),

    owned AS (SELECT DISTINCT game_title FROM sqlite_scan('library.db', 'steam_inventory')),

    raw_data AS (
        SELECT
            m.title,
            m.salePrice,
            m.normalPrice, -- [BARU] Ambil harga asli
            m.cheapestPrice,
            m.savings,
            m.dealRating,
            m.metacriticScore,
            r.rate,
            fin.total as budget
        FROM read_csv('s3://lakehouse/bronze/market_data.csv', header=True) m
        CROSS JOIN rate r
        CROSS JOIN fin
    ),

    -- Hitung semua logika di sini dulu (CTE)
    final_calc AS (
        SELECT
            title as Game,
            (CAST(normalPrice AS FLOAT) * rate) as Harga_Asli_IDR, -- [BARU] Hitung Harga Asli Rupiah
            (CAST(salePrice AS FLOAT) * rate) as Harga_IDR,
            (CAST(cheapestPrice AS FLOAT) * rate) as Lowest_IDR,
            CAST(savings AS FLOAT) as Diskon_Persen,
            CAST(dealRating AS FLOAT) as Skor_Deal,
            CAST(metacriticScore AS FLOAT) as Score_Quality,
            ((CAST(savings AS FLOAT) * 0.4) + ((CAST(dealRating AS FLOAT) * 10) * 0.3) + (CAST(metacriticScore AS FLOAT) * 0.3)) as Skor_Akhir,
            budget as Budget_User,

            CASE
                WHEN title IN (SELECT game_title FROM owned) THEN '❌ SUDAH PUNYA'
                WHEN (CAST(salePrice AS FLOAT) * rate) > budget THEN '⏳ NABUNG DULU'
                WHEN CAST(salePrice AS FLOAT) <= (CAST(cheapestPrice AS FLOAT) + 0.01) THEN '💎 ALL TIME LOW (Sikat!)'
                WHEN ((CAST(savings AS FLOAT) * 0.4) + ((CAST(dealRating AS FLOAT) * 10) * 0.3) + (CAST(metacriticScore AS FLOAT) * 0.3)) >= 75 THEN '✅ GAS BELI (Good Deal)'
                WHEN ((CAST(savings AS FLOAT) * 0.4) + ((CAST(dealRating AS FLOAT) * 10) * 0.3) + (CAST(metacriticScore AS FLOAT) * 0.3)) >= 50 THEN '⚠️ PIKIR DULU'
                ELSE '⛔ SKIP (Skor Rendah)'
            END as Rekomendasi_AI
        FROM raw_data
    )

    -- Baru di-SELECT dan ORDER BY di luar CTE
    SELECT * FROM final_calc
    ORDER BY
        (CASE WHEN Rekomendasi_AI LIKE '%ALL TIME LOW%' THEN 1 ELSE 0 END) DESC,
        Skor_Akhir DESC
    """

    res_df = con.execute(query).df()
    status_box.update(label="Analisis Selesai!", state="complete")

    return res_df, rate_idr, current_month_id


# --- UI ---
st.title("🤖 Smart Game Procurement (Historical Low)")
st.caption(
    "Sistem Cerdas dengan deteksi 'All Time Low Price' dari CheapShark Database."
)

with st.sidebar:
    st.header("Login")
    user_steam_id = st.text_input("SteamID64", help="765611980XXXXXXXX")

    st.info("""
    **Legenda Keputusan Preskriptif:**
    - 💎 **ALL TIME LOW**: Harga termurah dalam sejarah (Wajib Beli).
    - ✅ **GAS BELI**: Total skor bagus (>75), worth it.
    - ⚠️ **PIKIR DULU**: Total skor rata-rata (50-75).
    - ⏳ **NABUNG DULU**: Budget tabungan belum cukup.
    - ⛔ **SKIP**: Deal buruk atau game kurang bagus.
    - ❌ **SUDAH PUNYA**: Terdeteksi di Steam Library.
    """)

if st.button("🧠 Cek Harga Terendah & Rekomendasi", type="primary"):
    if not user_steam_id:
        st.error("SteamID wajib diisi!")
    else:
        result, current_rate, bulan_skrg = run_pipeline(user_steam_id)

        if result is not None and not result.empty:
            budget_now = result["Budget_User"].iloc[0]

            col1, col2, col3 = st.columns(3)
            col1.metric("Kurs USD", format_idr(current_rate))
            col2.metric("Total Budget", format_idr(budget_now))

            # Hitung jumlah All Time Low
            atl_count = len(
                result[result["Rekomendasi_AI"].str.contains("ALL TIME LOW")]
            )
            col3.metric(
                "🔥 All Time Low Found", f"{atl_count} Game", delta="Best Price!"
            )

            st.divider()

            # Styling Warna
            def color_coding(val):
                if "ALL TIME LOW" in val:
                    return "background-color: #d1e7dd; color: #0f5132; font-weight: bold; border: 1px solid green;"
                elif "GAS BELI" in val:
                    return "background-color: #e2e3e5; color: #41464b"
                elif "NABUNG" in val:
                    return "background-color: #f8d7da; color: #842029"
                elif "PIKIR" in val:
                    return "background-color: #fff3cd; color: #664d03"
                elif "SUDAH" in val:
                    return (
                        "background-color: #d3d3d3; color: #555555; font-style: italic;"
                    )
                return ""

            # Tampilan Dataframe
            st.dataframe(
                result.style.applymap(color_coding, subset=["Rekomendasi_AI"]),
                column_config={
                    "Game": st.column_config.TextColumn("Judul Game"),
                    "Harga_Asli_IDR": st.column_config.NumberColumn(
                        "Harga Asli", format="Rp %d"
                    ),
                    "Diskon_Persen": st.column_config.ProgressColumn(
                        "Diskon", format="%.0f%%", min_value=0, max_value=100
                    ),
                    "Harga_IDR": st.column_config.NumberColumn(
                        "Harga Saat Ini", format="Rp %d"
                    ),
                    "Lowest_IDR": st.column_config.NumberColumn(
                        "Lowest Price (Histori)",
                        format="Rp %d",
                    ),
                    "Score_Quality": st.column_config.NumberColumn(
                        "Metacritic", format="%d"
                    ),
                    "Skor_Deal": st.column_config.NumberColumn(
                        "Deal Rating",
                        format="%.1f / 10",
                    ),
                    "Skor_Akhir": st.column_config.NumberColumn(
                        "Total Skor AI",
                        format="%.1f",
                        min_value=0,
                        max_value=100,
                        help="Gabungan: Diskon (40%) + Deal Rating (30%) + Metacritic (30%)",
                    ),
                    "Rekomendasi_AI": st.column_config.TextColumn(
                        "Keputusan Preskriptif"
                    ),
                    "Budget_User": None,
                },
                use_container_width=True,
                height=600,
            )
