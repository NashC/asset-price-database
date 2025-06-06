"""
Tests for quality control module.
"""

import pytest
import pandas as pd
from datetime import datetime

from etl.qc import (
    validate_schema, detect_duplicates, score_quality,
    validate_price_ranges, generate_quality_report
)


class TestSchemaValidation:
    """Test schema validation functions."""
    
    def test_validate_schema_success(self):
        """Test successful schema validation."""
        df = pd.DataFrame({
            'symbol': ['AAPL'],
            'date_str': ['2024-01-01'],
            'open_str': ['100.0'],
            'high_str': ['102.0'],
            'low_str': ['99.0'],
            'close_str': ['101.0'],
            'volume_str': ['1000000']
        })
        
        # Should not raise exception
        validate_schema(df)
    
    def test_validate_schema_missing_columns(self):
        """Test schema validation with missing columns."""
        df = pd.DataFrame({
            'symbol': ['AAPL'],
            'date_str': ['2024-01-01']
            # Missing required columns
        })
        
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_schema(df)
    
    def test_validate_schema_empty_dataframe(self):
        """Test schema validation with empty DataFrame."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="DataFrame is empty"):
            validate_schema(df)


class TestDuplicateDetection:
    """Test duplicate detection functions."""
    
    def test_detect_duplicates_none(self):
        """Test duplicate detection with no duplicates."""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'AAPL'],
            'date_str': ['2024-01-01', '2024-01-02']
        })
        
        duplicates = detect_duplicates(df, ['symbol', 'date_str'])
        assert len(duplicates) == 0
    
    def test_detect_duplicates_found(self):
        """Test duplicate detection with duplicates present."""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'AAPL', 'MSFT'],
            'date_str': ['2024-01-01', '2024-01-01', '2024-01-01']
        })
        
        duplicates = detect_duplicates(df, ['symbol', 'date_str'])
        assert len(duplicates) == 2  # Both AAPL rows are duplicates
        assert all(duplicates['symbol'] == 'AAPL')


class TestQualityScoring:
    """Test quality scoring functions."""
    
    def test_score_quality_perfect_data(self):
        """Test quality scoring with perfect data."""
        df = pd.DataFrame({
            'symbol': ['AAPL'] * 5,
            'date_str': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'open_str': ['100.0', '101.0', '102.0', '103.0', '104.0'],
            'high_str': ['102.0', '103.0', '104.0', '105.0', '106.0'],
            'low_str': ['99.0', '100.0', '101.0', '102.0', '103.0'],
            'close_str': ['101.0', '102.0', '103.0', '104.0', '105.0'],
            'volume_str': ['1000000'] * 5
        })
        
        score = score_quality(df)
        assert score >= 90.0  # Should be high quality
    
    def test_score_quality_poor_data(self):
        """Test quality scoring with poor quality data."""
        df = pd.DataFrame({
            'symbol': [None, 'AAPL', 'AAPL'],
            'date_str': ['invalid-date', '2024-01-01', '2024-01-01'],  # Duplicate + invalid
            'open_str': ['not-a-number', '100.0', '100.0'],
            'high_str': ['99.0', '102.0', '102.0'],  # First row has high < open
            'low_str': ['101.0', '99.0', '99.0'],   # First row has low > open
            'close_str': ['100.0', '101.0', '101.0'],
            'volume_str': ['1000000', '1000000', '1000000']
        })
        
        score = score_quality(df)
        assert score < 50.0  # Should be low quality
    
    def test_score_quality_empty_dataframe(self):
        """Test quality scoring with empty DataFrame."""
        df = pd.DataFrame()
        score = score_quality(df)
        assert score == 0.0


class TestPriceValidation:
    """Test price range validation functions."""
    
    def test_validate_price_ranges_clean_data(self):
        """Test price validation with clean data."""
        df = pd.DataFrame({
            'symbol': ['AAPL'] * 3,
            'date_str': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'open_str': ['100.0', '101.0', '102.0'],
            'high_str': ['102.0', '103.0', '104.0'],
            'low_str': ['99.0', '100.0', '101.0'],
            'close_str': ['101.0', '102.0', '103.0'],
            'volume_str': ['1000000', '1100000', '1200000']
        })
        
        results = validate_price_ranges(df)
        
        assert len(results['negative_prices']) == 0
        assert len(results['extreme_values']) == 0
        assert len(results['zero_volumes']) == 0
    
    def test_validate_price_ranges_with_issues(self):
        """Test price validation with various issues."""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'MSFT', 'GOOGL'],
            'date_str': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'open_str': ['-10.0', '100.0', '200.0'],  # Negative price
            'high_str': ['-5.0', '150.0', '400.0'],   # Negative + extreme change
            'low_str': ['-15.0', '90.0', '180.0'],
            'close_str': ['-8.0', '149.0', '399.0'],
            'volume_str': ['1000000', '0', '1200000']  # Zero volume
        })
        
        results = validate_price_ranges(df)
        
        assert len(results['negative_prices']) > 0
        assert len(results['zero_volumes']) > 0


class TestQualityReport:
    """Test quality report generation."""
    
    def test_generate_quality_report(self):
        """Test comprehensive quality report generation."""
        df = pd.DataFrame({
            'symbol': ['AAPL'] * 5,
            'date_str': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'open_str': ['100.0', '101.0', '102.0', '103.0', '104.0'],
            'high_str': ['102.0', '103.0', '104.0', '105.0', '106.0'],
            'low_str': ['99.0', '100.0', '101.0', '102.0', '103.0'],
            'close_str': ['101.0', '102.0', '103.0', '104.0', '105.0'],
            'volume_str': ['1000000'] * 5
        })
        
        report = generate_quality_report(df, "test_batch")
        
        assert report['batch_name'] == "test_batch"
        assert report['row_count'] == 5
        assert report['schema_valid'] is True
        assert 'quality_score' in report
        assert 'duplicates' in report
        assert 'outliers' in report
        assert 'summary_stats' in report
        assert 'timestamp' in report
    
    def test_generate_quality_report_with_schema_errors(self):
        """Test quality report with schema validation errors."""
        df = pd.DataFrame({
            'symbol': ['AAPL'],
            # Missing required columns
        })
        
        report = generate_quality_report(df, "test_batch_invalid")
        
        assert report['schema_valid'] is False
        assert 'schema_errors' in report 