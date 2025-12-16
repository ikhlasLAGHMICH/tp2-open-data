import pytest
import pandas as pd
import numpy as np
from pipeline.transformer import DataTransformer


class TestDataTransformer:
    """Tests pour DataTransformer."""
    
    @pytest.fixture
    def sample_df(self):
        """DataFrame de test."""
        return pd.DataFrame({
            'code': ['001', '002', '001', '003'],
            'name': ['  Test  ', None, 'Test', 'Other'],
            'value': [10.0, None, 10.0, 100.0],
        })
    
    def test_remove_duplicates(self, sample_df):
        """Test la suppression des doublons."""
        transformer = DataTransformer(sample_df)
        result = transformer.remove_duplicates(['code']).get_result()
        
        assert len(result) == 3
        assert result['code'].nunique() == 3
    
    def test_handle_missing_values_median(self, sample_df):
        """Test le remplacement par la médiane."""
        transformer = DataTransformer(sample_df)
        result = transformer.handle_missing_values(numeric_strategy='median').get_result()
        
        assert result['value'].isnull().sum() == 0
    
    def test_normalize_text(self, sample_df):
        """Test la normalisation du texte."""
        transformer = DataTransformer(sample_df)
        result = transformer.normalize_text_columns(['name']).get_result()
        
        # Vérifie que les espaces sont supprimés et en minuscules
        assert 'test' in result['name'].values
    
    def test_chaining(self, sample_df):
        """Test le chaînage des transformations."""
        transformer = DataTransformer(sample_df)
        result = (
            transformer
            .remove_duplicates()
            .handle_missing_values()
            .get_result()
        )
        
        assert len(transformer.transformations_applied) >= 2