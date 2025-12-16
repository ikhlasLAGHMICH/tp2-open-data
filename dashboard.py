import streamlit as st
import pandas as pd
import glob
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Dashboard Qualité Données", layout="wide")

st.title(" Dashboard Qualité & Données")

# 1. Sélecteur de fichier
processed_dir = Path("data/processed")
files = list(processed_dir.glob("*.parquet"))

if not files:
    st.error("Aucun fichier de données trouvé. Lancez le pipeline d'abord !")
    st.stop()

# Trier par date de création (le plus récent en premier)
files = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)
selected_file = st.selectbox("Choisir un dataset :", files, format_func=lambda x: x.name)

# 2. Chargement des données
df = pd.read_parquet(selected_file)

# 3. Métriques clés (KPIs)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Produits", len(df))
col2.metric("Note Nutriscore A/B", len(df[df['nutriscore_grade'].isin(['a', 'b'])]))

if 'is_geocoded' in df.columns:
    geo_count = df['is_geocoded'].sum()
    col3.metric("Géocodés", f"{geo_count} ({geo_count/len(df)*100:.1f}%)")
else:
    col3.metric("Géocodés", "N/A")

# 4. Graphiques
st.subheader("📊 Analyse Nutritionnelle")
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    if 'nutriscore_grade' in df.columns:
        counts = df['nutriscore_grade'].value_counts()
        fig = px.bar(counts, title="Distribution Nutriscore")
        st.plotly_chart(fig, use_container_width=True)

with chart_col2:
    if 'sugars_100g' in df.columns and 'fat_100g' in df.columns:
        fig = px.scatter(df, x='sugars_100g', y='fat_100g', 
                         hover_data=['product_name'], color='nutriscore_grade',
                         title="Sucre vs Gras")
        st.plotly_chart(fig, use_container_width=True)

# 5. Carte (si géocodé)
if 'latitude' in df.columns and 'longitude' in df.columns:
    st.subheader("🌍 Carte des Magasins/Origines")
    # Filtrer les points valides
    map_df = df.dropna(subset=['latitude', 'longitude'])
    if not map_df.empty:
        st.map(map_df, latitude='latitude', longitude='longitude')
    else:
        st.info("Pas de données géographiques valides à afficher.")

# 6. Tableau de données
st.subheader("📋 Données Brutes")
st.dataframe(df)