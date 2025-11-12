import streamlit as st
import pandas as pd
import os
from io import BytesIO
import csv

st.title("🚀 EOM/EOC Report")

# -------------------------------
# TAB 1: VOD/TVE Processing
# -------------------------------
tabs = st.tabs(["VOD/TVE", "Combining Sheets"])

with tabs[0]:
    st.header("🎬 VOD / TVE File Processor")

    def replace_networks(network, channels_dict):
        return channels_dict.get(network, network)

    def get_network_list(file_path, filename):
        path = os.path.join(file_path, filename)
        if not os.path.isfile(path):
            st.error(f"❌ File not found: {path}")
            return []
        with open(path, mode="r") as file:
            csv_reader = csv.reader(file)
            names_list = [row[0] for row in csv_reader if row]
        return names_list

    def get_ff_rename_dict(file_path, filename):
        path = os.path.join(file_path, filename)
        if not os.path.isfile(path):
            st.error(f"❌ File not found: {path}")
            return {}
        channels_dict = {}
        with open(path, mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) >= 2:
                    channels_dict[row[0].strip()] = row[1].strip()
        return channels_dict

    def vod_extract_data(file_path, file_name, network_list, channels_dict):
        path = os.path.join(file_path, file_name)
        if not os.path.isfile(path):
            st.error(f"❌ File not found: {path}")
            return

        try:
            df = pd.read_csv(path, low_memory=False, encoding="latin1")
        except Exception:
            df = pd.read_csv(path, low_memory=False, skiprows=4, encoding="utf-8")

        # Clean 'Net Counted Ads'
        if df["Net Counted Ads"].dtype == object and df["Net Counted Ads"].str.contains(",").any():
            df["Net Counted Ads"] = df["Net Counted Ads"].str.replace(",", "")
        df["Net Counted Ads"] = df["Net Counted Ads"].astype(int)

        # Add 'Networks'
        df["Networks"] = df["Video Group Name"].apply(
            lambda x: next((n for n in network_list if n == x), "Unknown")
        )
        df["Networks"] = df["Networks"].apply(lambda x: replace_networks(x, channels_dict))
        df = df[df["Networks"] != "Unknown"]

        grouped_df = df.groupby("Networks")["Net Counted Ads"].sum().reset_index()

        output_file = f"{os.path.splitext(file_name)[0]}_VOD_Output.csv"
        grouped_df.to_csv(output_file, index=False, encoding="utf-8")
        st.success(f"✅ VOD output file created: {output_file}")

    def tve_extract_data(file_path, file_name, network_list):
        path = os.path.join(file_path, file_name)
        if not os.path.isfile(path):
            st.error(f"❌ File not found: {path}")
            return

        try:
            df = pd.read_csv(path, low_memory=False, encoding="utf-8")
        except Exception:
            df = pd.read_csv(path, low_memory=False, skiprows=4, encoding="utf-8")

        if df["Net Counted Ads"].dtype == object and df["Net Counted Ads"].str.contains(",").any():
            df["Net Counted Ads"] = df["Net Counted Ads"].str.replace(",", "").astype(int)

        stbSS = ["cox_legacy_section_live", "cox_watermark_c2_section_live"]
        df = df[~df["Site Section Name"].isin(stbSS)]
        df["Site Section Name"] = df["Site Section Name"].str.replace("A+E", "AE", regex=False)

        df["Networks"] = df["Site Section Name"].apply(
            lambda x: next((n for n in network_list if n.lower() in x.lower()), "Unknown")
        )

        grouped_df = df.groupby("Networks")["Net Counted Ads"].sum().reset_index()
        grouped_df["Networks"] = grouped_df["Networks"].replace("A&E", "AE")

        output_file = f"{os.path.splitext(file_name)[0]}_TVE_Output.csv"
        grouped_df.to_csv(output_file, index=False, encoding="utf-8")
        st.success(f"✅ TVE output file created: {output_file}")

    # ---- Streamlit Inputs ----
    file_path = os.getcwd() + "/"
    file_name = st.text_input("📄 Enter input filename (e.g., Test.csv):", "")
    report_type = st.selectbox("📊 Select report type:", ["VOD", "TVE", "VOD/TVE"])

    if st.button("🚀 Run Extraction"):
        if not file_name:
            st.warning("⚠️ Please enter a valid file name.")
        else:
            video_group = get_network_list(file_path, "VideoGroups.csv")
            network_list = get_network_list(file_path, "tve_networks.csv")
            channels_dict = get_ff_rename_dict(file_path, "FF_VideoGroups.csv")

            if report_type == "VOD":
                vod_extract_data(file_path, file_name, video_group, channels_dict)
            elif report_type == "TVE":
                tve_extract_data(file_path, file_name, network_list)
            else:
                vod_extract_data(file_path, file_name, video_group, channels_dict)
                tve_extract_data(file_path, file_name, network_list)

# -------------------------------
# TAB 2: Combining Sheets
# -------------------------------
with tabs[1]:
    Addressable_keywords = ["VOD", "LSA", "Delivery", "Reach-Frequency", "Unique RF", "Daily", "Hourly", "Creative", "Geo"]
    Non_Addressable_keywords = ["VOD","TVE", "Delivery", "Reach-Frequency", "Unique RF", "Daily", "Hourly", "Creative",
                            "Geo"]
    st.header("📊 Multi-Excel File Combiner")
    campaign_type = st.selectbox("Select campaign type:", ["Addressable","Non-Addressable"])
    uploaded_files = st.file_uploader(
        "Upload multiple CSV or Excel files", type=["csv", "xlsx"], accept_multiple_files=True
    )
    if st.button("🚀 Run File Processing"):
        table_df = pd.DataFrame(columns=["Tab Name","Sum Of Net Count Ads"])
        def multifiles_to_one(KEYWORDS,uploaded_files,table_df):
            if not uploaded_files:
                st.warning("⚠️ Please upload at least one file.")
            else:
                data_by_keyword = {key: [] for key in KEYWORDS}

                for uploaded_file in uploaded_files:
                    file_name = uploaded_file.name
                    st.write(f"🔍 Processing file: `{file_name}`")

                    try:
                        if file_name.endswith(".csv"):
                            df = pd.read_csv(uploaded_file)
                        else:
                            df = pd.read_excel(uploaded_file)
                    except Exception as e:
                        st.warning(f"❌ Failed to read {file_name}: {e}")
                        continue

                    if any("Unnamed" in str(c) or pd.isna(c) for c in df.columns):
                        uploaded_file.seek(0)
                        try:
                            if file_name.endswith(".csv"):
                                df = pd.read_csv(uploaded_file, skiprows=4)
                            else:
                                df = pd.read_excel(uploaded_file, skiprows=4)
                            # st.info("↪️ Header issue detected — skipped first 4 rows.")
                        except Exception as e:
                            st.warning(f"⚠️ Could not re-read {file_name}: {e}")
                            continue

                    matched = False
                    for key in KEYWORDS:
                        if key.lower() in file_name.lower():
                            if key.lower() == "lsa" and "Placement Name" in df.columns:
                                df = df[~df["Placement Name"].str.contains("VOD", case=False, na=False)]
                                cols = [c for c in ["Television Network Name", "Net Counted Ads", "Video Ads 100% Complete"] if c in df.columns]
                                df = df[cols] if cols else df
                            if key in ["VOD", "TVE","LSA", "Delivery", "Daily", "Hourly", "Creative", "Geo"]:
                                # st.success(f"Sum of Net Counted Ads for {key}: {df['Net Counted Ads'].sum():,}")
                                # Example: you already have df and table_df
                                sum_net_count_ads = df["Net Counted Ads"].sum()

                                # Create a new row as a dictionary
                                new_row = {
                                    "Tab Name": key,
                                    "Sum Of Net Count Ads": sum_net_count_ads
                                }
                                # Append the new row to table_df
                                table_df = pd.concat([table_df, pd.DataFrame([new_row])], ignore_index=True)

                            data_by_keyword[key].append(df)
                            matched = True
                            break

                    if not matched:
                        st.warning(f"⚠️ No matching keyword found in file name: {file_name}")
                if {"VOD", "LSA"}.issubset(set(table_df["Tab Name"])):
                    # Calculate combined sum
                    combined_sum = table_df.loc[table_df["Tab Name"].isin(["VOD", "LSA"]), "Sum Of Net Count Ads"].sum()

                    # Remove those rows
                    table_df = table_df[~table_df["Tab Name"].isin(["VOD", "LSA"])]

                    # Add the combined row
                    table_df.loc[len(table_df)] = ["VOD & LSA", combined_sum]

                # Reset index (optional)
                table_df = table_df.reset_index(drop=True)
                st.table(table_df)
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    for key, dfs in data_by_keyword.items():
                        if dfs:
                            combined_df = pd.concat(dfs, ignore_index=True)
                            combined_df.to_excel(writer, sheet_name=key[:31], index=False)
                            # st.success(f"✅ Added {len(dfs)} file(s) to '{key}' tab")

            st.download_button(
                label="⬇️ Download Combined Excel",
                data=output.getvalue(),
                file_name="combined_output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        if campaign_type == "Addressable":
            multifiles_to_one(Addressable_keywords,uploaded_files,table_df)
        else:
            multifiles_to_one(Non_Addressable_keywords, uploaded_files,table_df)