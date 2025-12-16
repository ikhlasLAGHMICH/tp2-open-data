import json
import pandas as pd
from datetime import datetime
from pathlib import Path

from .config import RAW_DIR, PROCESSED_DIR


def save_raw_json(data: list[dict], name: str) -> Path:
    """Sauvegarde les données brutes en JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = RAW_DIR / f"{name}_{timestamp}.json"
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    size_kb = filepath.stat().st_size / 1024
    print(f"   💾 Brut: {filepath.name} ({size_kb:.1f} KB)")
    
    return filepath


def save_parquet(df: pd.DataFrame, name: str) -> Path:
    """Sauvegarde le DataFrame en Parquet."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = PROCESSED_DIR / f"{name}_{timestamp}.parquet"
    
    df.to_parquet(filepath, index=False, compression="snappy")
    
    size_kb = filepath.stat().st_size / 1024
    print(f"   💾 Parquet: {filepath.name} ({size_kb:.1f} KB)")
    
    return filepath


def load_parquet(filepath: str | Path) -> pd.DataFrame:
    """Charge un fichier Parquet."""
    return pd.read_parquet(filepath)
