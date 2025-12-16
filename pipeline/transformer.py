"""Module de transformation et nettoyage."""
import pandas as pd
import numpy as np
from typing import Callable, List
from litellm import completion
from dotenv import load_dotenv
import os

from .models import Product

load_dotenv()

class DataTransformer:
    """Transforme et nettoie les données."""
    
    def __init__(self, df: pd.DataFrame):
        # On travaille sur une copie pour ne pas casser l'original
        self.df = df.copy()
        self.transformations_applied = []
    
    def remove_duplicates(self, subset: List[str] = None) -> 'DataTransformer':
        """Supprime les doublons basés sur l'ID (code)."""
        initial = len(self.df)
        
        # Si pas de colonne spécifiée, on cherche 'code' ou on prend la première
        if subset is None:
            subset = ['code'] if 'code' in self.df.columns else [self.df.columns[0]]
        
        self.df = self.df.drop_duplicates(subset=subset, keep='first')
        removed = initial - len(self.df)
        
        if removed > 0:
            self.transformations_applied.append(f"Doublons supprimés: {removed}")
        return self
    
    def handle_missing_values(
        self, 
        numeric_strategy: str = 'median',
        text_strategy: str = 'unknown'
    ) -> 'DataTransformer':
        """Gère les valeurs manquantes."""
        
        # --- CORRECTIF : FORCER LA CONVERSION NUMERIQUE ---
        # On liste les colonnes qui DOIVENT être des nombres pour éviter les erreurs de type
        numeric_targets = ['energy_100g', 'sugars_100g', 'fat_100g', 'salt_100g', 'nova_group', 'geocoding_score']
        
        for col in numeric_targets:
            if col in self.df.columns:
                # errors='coerce' transforme les textes bizarres en NaN (vide)
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        # --------------------------------------------------

        # 1. Colonnes numériques (maintenant qu'elles sont propres)
        num_cols = self.df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            if numeric_strategy == 'median':
                fill_value = self.df[col].median()
            elif numeric_strategy == 'mean':
                fill_value = self.df[col].mean()
            elif numeric_strategy == 'zero':
                fill_value = 0
            else:
                fill_value = None
            
            if fill_value is not None:
                null_count = self.df[col].isnull().sum()
                if null_count > 0:
                    self.df[col] = self.df[col].fillna(fill_value)
                    self.transformations_applied.append(f"{col}: {null_count} nulls → {fill_value:.2f}")
        
        # 2. Colonnes texte
        text_cols = self.df.select_dtypes(include=['object']).columns
        for col in text_cols:
            null_count = self.df[col].isnull().sum()
            if null_count > 0:
                self.df[col] = self.df[col].fillna(text_strategy)
                self.transformations_applied.append(f"{col}: {null_count} nulls → '{text_strategy}'")
        
        return self
    
    def normalize_text_columns(self, columns: List[str] = None) -> 'DataTransformer':
        """Normalise les colonnes texte (minuscules, sans espaces inutiles)."""
        if columns is None:
            # Par défaut, toutes les colonnes texte
            columns = self.df.select_dtypes(include=['object']).columns.tolist()
        
        for col in columns:
            if col in self.df.columns:
                # str.strip() enlève les espaces avant/après
                # str.lower() met tout en minuscule
                self.df[col] = self.df[col].astype(str).str.strip().str.lower()
        
        self.transformations_applied.append(f"Normalisation texte: {columns}")
        return self
    
    def filter_outliers(
        self, 
        columns: List[str], 
        method: str = 'iqr',
        threshold: float = 1.5
    ) -> 'DataTransformer':
        """Filtre les valeurs aberrantes (ex: sucre > 100g)."""
        initial = len(self.df)
        
        for col in columns:
            if col not in self.df.columns:
                continue
            
            # Méthode Interquartile Range (classique)
            if method == 'iqr':
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - threshold * IQR
                upper = Q3 + threshold * IQR
                self.df = self.df[(self.df[col] >= lower) & (self.df[col] <= upper)]
            
            # Méthode Z-Score (écart-type)
            elif method == 'zscore':
                mean = self.df[col].mean()
                std = self.df[col].std()
                if std > 0:
                    self.df = self.df[np.abs((self.df[col] - mean) / std) < threshold]
        
        removed = initial - len(self.df)
        if removed > 0:
            self.transformations_applied.append(f"Outliers filtrés ({method}): {removed}")
        return self
    
    def add_derived_columns(self) -> 'DataTransformer':
        """Ajoute des colonnes calculées utiles pour l'analyse."""
        
        # Exemple 1 : Catégorie de sucre (Faible/Elevé)
        if 'sugars_100g' in self.df.columns:
            self.df['sugars_100g'] = pd.to_numeric(self.df['sugars_100g'], errors='coerce')
            self.df['sugar_category'] = pd.cut(
                self.df['sugars_100g'],
                bins=[-1, 5, 15, 30, float('inf')], # -1 pour inclure le 0
                labels=['faible', 'modéré', 'élevé', 'très_élevé']
            )
            self.transformations_applied.append("Ajout: sugar_category")
        
        # Exemple 2 : Flag "Est géocodé ?"
        if 'geocoding_score' in self.df.columns:
            self.df['is_geocoded'] = self.df['geocoding_score'] >= 0.5
            self.transformations_applied.append("Ajout: is_geocoded")
        
        return self
    
    def generate_ai_transformations(self) -> str:
        """Demande à l'IA (OLLAMA LOCAL) du code Python."""
        
        context = f"""
        Dataset avec {len(self.df)} lignes.
        Colonnes: {list(self.df.columns)}
        Types: {self.df.dtypes.to_dict()}
        
        Transformations déjà appliquées:
        {self.transformations_applied}
        """
        
        try:
            # --- MODIFICATION ICI ---
            response = completion(
                model="ollama/llama3.2", # Ou "ollama/mistral"
                api_base="http://localhost:11434",
                messages=[
                    {
                        "role": "system",
                        "content": "Tu es un expert Pandas. Génère UNIQUEMENT du code Python exécutable pour nettoyer ce dataset."
                    },
                    {
                        "role": "user",
                        "content": f"{context}\n\nQuelles transformations supplémentaires recommandes-tu ? Donne le code."
                    }
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"# Erreur Ollama : {e}"
    
    def get_result(self) -> pd.DataFrame:
        """Retourne le DataFrame transformé."""
        return self.df
    
    def get_summary(self) -> str:
        """Retourne un résumé lisible des actions effectuées."""
        if not self.transformations_applied:
            return "Aucune transformation appliquée."
        return "\n".join([f"• {t}" for t in self.transformations_applied])