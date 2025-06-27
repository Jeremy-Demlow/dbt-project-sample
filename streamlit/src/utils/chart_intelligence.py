"""
Dynamic Chart Intelligence System for Cortex Analyst

This module provides intelligent visualization recommendations based on data characteristics,
statistical properties, and established data visualization best practices.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from scipy import stats
import warnings

warnings.filterwarnings('ignore')

@dataclass
class UserIntent:
    """Represents parsed user intent from query context"""
    primary_intent: str
    secondary_intents: List[str] = field(default_factory=list)
    analytical_goal: str = ""
    comparison_type: Optional[str] = None
    temporal_focus: bool = False
    composition_focus: bool = False
    relationship_focus: bool = False
    distribution_focus: bool = False
    confidence: float = 0.0
    
@dataclass
class AggregationStrategy:
    """Represents data aggregation requirements"""
    needs_aggregation: bool
    group_by_columns: List[str] = field(default_factory=list)
    aggregate_columns: Dict[str, str] = field(default_factory=dict)  # column -> method
    reasoning: str = ""

class DataType(Enum):
    """Enumeration of data types for chart intelligence"""
    NUMERIC_CONTINUOUS = "numeric_continuous"
    NUMERIC_DISCRETE = "numeric_discrete"
    CATEGORICAL = "categorical"
    TEMPORAL = "temporal"
    TEXT = "text"
    BOOLEAN = "boolean"

class ChartType(Enum):
    """Enumeration of supported chart types"""
    BAR = "bar"
    HORIZONTAL_BAR = "horizontal_bar"
    LINE = "line"
    AREA = "area"
    SCATTER = "scatter"
    PIE = "pie"
    HISTOGRAM = "histogram"
    BOX = "box"
    HEATMAP = "heatmap"
    TABLE = "table"
    STACKED_BAR = "stacked_bar"
    STACKED_AREA = "stacked_area"
    TREEMAP = "treemap"
    GROUPED_BAR = "grouped_bar"

@dataclass
class ColumnAnalysis:
    """Analysis results for a single column"""
    name: str
    data_type: DataType
    cardinality: int
    null_count: int
    null_percentage: float
    unique_values: int
    sample_values: List[Any] = field(default_factory=list)
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    top_categories: List[Tuple[Any, int]] = field(default_factory=list)
    contains_identifiers: bool = False

@dataclass 
class DatasetAnalysis:
    """Complete analysis of a dataset"""
    row_count: int
    column_count: int
    columns: Dict[str, ColumnAnalysis]
    temporal_columns: List[str] = field(default_factory=list)
    numeric_columns: List[str] = field(default_factory=list) 
    categorical_columns: List[str] = field(default_factory=list)
    needs_aggregation: bool = False
    aggregation_suggestions: Dict[str, str] = field(default_factory=dict)
    data_patterns: List[str] = field(default_factory=list)
    complexity_score: float = 0.0

@dataclass
class ChartRecommendation:
    """A single chart recommendation with metadata"""
    chart_type: ChartType
    confidence_score: float
    reasoning: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    color_by: Optional[str] = None
    aggregation_method: Optional[str] = None
    accessibility_notes: str = ""
    detailed_explanation: str = ""
    data_requirements: Dict[str, Any] = field(default_factory=dict)
    perceptual_effectiveness_score: float = 0.0
    user_intent_match: float = 0.0
    warnings: List[str] = field(default_factory=list)

class EnhancedIntentAnalyzer:
    """Advanced intent analysis for user queries"""
    
    def __init__(self):
        self.intent_patterns = {
            'distribution': ['distribution', 'spread', 'frequency', 'histogram', 'range', 'values'],
            'comparison': ['compare', 'versus', 'vs', 'against', 'between', 'difference'],
            'trend': ['trend', 'over time', 'time series', 'temporal', 'change', 'evolution'],
            'composition': ['composition', 'breakdown', 'proportion', 'percentage', 'share', 'makeup'],
            'relationship': ['relationship', 'correlation', 'association', 'connection', 'analyze'],
            'ranking': ['top', 'bottom', 'highest', 'lowest', 'best', 'worst', 'rank'],
            'outlier': ['outlier', 'anomaly', 'unusual', 'extreme', 'exception'],
            'quartile': ['quartile', 'percentile', 'median', 'iqr', 'interquartile']
        }
        
    def analyze_user_intent(self, query_context: str) -> UserIntent:
        """Parse user query to extract analytical intent"""
        if not query_context:
            return UserIntent(primary_intent="explore", confidence=0.5)
            
        query_lower = query_context.lower()
        intent_scores = {}
        
        # Score each intent based on keyword matches
        for intent, keywords in self.intent_patterns.items():
            score = sum(1 for keyword in keywords if keyword in query_lower)
            if score > 0:
                intent_scores[intent] = score / len(keywords)
        
        if not intent_scores:
            return UserIntent(primary_intent="explore", confidence=0.3)
        
        # Find primary intent
        primary_intent = max(intent_scores.keys(), key=lambda x: intent_scores[x])
        secondary_intents = [intent for intent, score in intent_scores.items() 
                           if intent != primary_intent and score > 0.1]
        
        # Determine analytical goal
        analytical_goal = self._determine_analytical_goal(query_lower, primary_intent)
        
        # Set focus flags
        intent = UserIntent(
            primary_intent=primary_intent,
            secondary_intents=secondary_intents,
            analytical_goal=analytical_goal,
            temporal_focus='trend' in intent_scores or any(word in query_lower for word in ['time', 'day', 'week', 'month', 'year']),
            composition_focus='composition' in intent_scores,
            relationship_focus='relationship' in intent_scores,
            distribution_focus='distribution' in intent_scores,
            confidence=intent_scores[primary_intent]
        )
        
        return intent
    
    def _determine_analytical_goal(self, query_lower: str, primary_intent: str) -> str:
        """Determine the specific analytical goal from the query"""
        goal_patterns = {
            'identify_patterns': ['pattern', 'identify', 'find', 'discover'],
            'measure_performance': ['performance', 'kpi', 'metric', 'measure'],
            'compare_segments': ['segment', 'group', 'category', 'type'],
            'track_changes': ['change', 'evolution', 'progress', 'development'],
            'understand_composition': ['composition', 'makeup', 'structure', 'breakdown']
        }
        
        for goal, keywords in goal_patterns.items():
            if any(keyword in query_lower for keyword in keywords):
                return goal
        
        return f"explore_{primary_intent}"

class QueryResultAnalyzer:
    """Analyzes Cortex Analyst query results to extract visualization-relevant metadata"""
    
    def __init__(self):
        self.high_cardinality_threshold = 30  # Reduced from 50 per PRD recommendations
        self.sample_size = 10
        self.intent_analyzer = EnhancedIntentAnalyzer()
        
    def analyze_result_set(self, dataframe: pd.DataFrame, query_context: str = "") -> DatasetAnalysis:
        """Comprehensive analysis of query results"""
        if dataframe.empty:
            return DatasetAnalysis(0, 0, {})
            
        columns = {}
        temporal_columns = []
        numeric_columns = []
        categorical_columns = []
        
        # Analyze each column
        for col_name in dataframe.columns:
            column_analysis = self._analyze_column(dataframe[col_name], col_name)
            columns[col_name] = column_analysis
            
            # Categorize by type
            if column_analysis.data_type == DataType.TEMPORAL:
                temporal_columns.append(col_name)
            elif column_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
                numeric_columns.append(col_name)
            elif column_analysis.data_type == DataType.CATEGORICAL:
                categorical_columns.append(col_name)
        
        # Detect aggregation needs and patterns
        aggregation_analysis = self._analyze_aggregation_needs(dataframe, columns, query_context)
        data_patterns = self._detect_data_patterns(dataframe, columns)
        complexity_score = self._calculate_complexity_score(dataframe, columns)
        
        return DatasetAnalysis(
            row_count=len(dataframe),
            column_count=len(dataframe.columns),
            columns=columns,
            temporal_columns=temporal_columns,
            numeric_columns=numeric_columns,
            categorical_columns=categorical_columns,
            needs_aggregation=aggregation_analysis.needs_aggregation,
            aggregation_suggestions=aggregation_analysis.aggregate_columns,
            data_patterns=data_patterns,
            complexity_score=complexity_score
        )
    
    def _analyze_column(self, series: pd.Series, col_name: str) -> ColumnAnalysis:
        """Analyze a single column and return comprehensive metadata"""
        cardinality = series.nunique()
        null_count = series.isnull().sum()
        null_percentage = (null_count / len(series)) * 100
        unique_values = series.nunique()
        sample_values = series.dropna().head(self.sample_size).tolist()
        
        # Detect data type
        data_type = self._detect_data_type(series, col_name)
        
        analysis = ColumnAnalysis(
            name=col_name,
            data_type=data_type,
            cardinality=cardinality,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_values=unique_values,
            sample_values=sample_values
        )
        
        # Type-specific analysis
        if data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
            self._analyze_numeric_column(series, analysis)
        elif data_type == DataType.CATEGORICAL:
            self._analyze_categorical_column(series, analysis)
        
        analysis.contains_identifiers = self._is_identifier_column(series, col_name)
        
        return analysis
    
    def _detect_data_type(self, series: pd.Series, col_name: str) -> DataType:
        """Detect the semantic data type of a column"""
        col_lower = col_name.lower()
        
        # Priority 1: Check for obvious numeric/value columns first
        value_keywords = ['value', 'price', 'amount', 'cost', 'revenue', 'profit', 'ltv', 'clv', 'score', 'rating']
        is_value_column = any(value_keyword in col_lower for value_keyword in value_keywords)
        
        # Priority 2: Check for actual datetime types
        if pd.api.types.is_datetime64_any_dtype(series):
            return DataType.TEMPORAL
        
        # Priority 3: Check for numeric data (including value columns)
        if pd.api.types.is_numeric_dtype(series):
            if series.dtype in ['int64', 'int32', 'int16', 'int8']:
                unique_ratio = series.nunique() / len(series)
                if unique_ratio < 0.05 or series.nunique() < 20:
                    return DataType.NUMERIC_DISCRETE
                else:
                    return DataType.NUMERIC_CONTINUOUS
            else:
                return DataType.NUMERIC_CONTINUOUS
        
        # Priority 4: Try to convert to numeric if it's a value column
        if is_value_column and series.dtype == 'object':
            try:
                numeric_series = pd.to_numeric(series.dropna().head(20), errors='coerce')
                if not numeric_series.isna().all():
                    return DataType.NUMERIC_CONTINUOUS
            except (ValueError, TypeError):
                pass
        
        # Priority 5: Check for temporal column names and try to parse as dates
        temporal_keywords = ['date', 'time', 'day', 'month', 'year', 'timestamp', 'created', 'updated', 'modified']
        
        if not is_value_column and any(keyword in col_lower for keyword in temporal_keywords):
            # Try to convert to datetime
            try:
                pd.to_datetime(series.dropna().head(10))
                return DataType.TEMPORAL
            except (ValueError, TypeError):
                pass
        
        # Priority 6: Check for boolean
        if series.dtype == 'bool' or series.nunique() == 2:
            return DataType.BOOLEAN
        
        # Priority 7: Check for high cardinality text
        if series.dtype == 'object':
            unique_ratio = series.nunique() / len(series)
            if unique_ratio > 0.9 and series.nunique() > 100:
                return DataType.TEXT
            # For object dtype with reasonable cardinality, it's categorical
            return DataType.CATEGORICAL
        
        # Default fallback for any unhandled cases
        return DataType.CATEGORICAL
    
    def _analyze_numeric_column(self, series: pd.Series, analysis: ColumnAnalysis):
        """Add numeric-specific analysis"""
        try:
            clean_series = pd.to_numeric(series, errors='coerce').dropna()
            if len(clean_series) > 0:
                analysis.mean = float(clean_series.mean())
                analysis.median = float(clean_series.median())
                analysis.std = float(clean_series.std())
                analysis.min_val = float(clean_series.min())
                analysis.max_val = float(clean_series.max())
        except Exception:
            pass
    
    def _analyze_categorical_column(self, series: pd.Series, analysis: ColumnAnalysis):
        """Add categorical-specific analysis"""
        try:
            value_counts = series.value_counts().head(10)
            analysis.top_categories = [(val, count) for val, count in value_counts.items()]
        except Exception:
            pass
    
    def _is_identifier_column(self, series: pd.Series, col_name: str) -> bool:
        """Check if column contains identifiers"""
        identifier_keywords = ['id', 'key', 'uuid', 'guid', 'hash', '_id', 'customer_id', 'user_id', 'session_id']
        col_lower = col_name.lower()
        
        # Check for identifier keywords in column name
        if any(keyword in col_lower for keyword in identifier_keywords):
            return True
        
        # Check for high uniqueness ratio (likely identifiers)
        unique_ratio = series.nunique() / len(series)
        if unique_ratio > 0.9 and series.nunique() > 100:
            return True
        
        # Check for moderate uniqueness with sequential patterns (like auto-incrementing IDs)
        if unique_ratio > 0.8 and series.nunique() > 50:
            # Check if it looks like sequential data (for numeric IDs)
            if pd.api.types.is_numeric_dtype(series):
                try:
                    sorted_values = series.dropna().sort_values()
                    if len(sorted_values) > 10:
                        # Check if values are mostly sequential
                        diffs = sorted_values.diff().dropna()
                        if diffs.mode().iloc[0] == 1:  # Most common difference is 1
                            return True
                except:
                    pass
        
        return False
    
    def _analyze_aggregation_needs(self, dataframe: pd.DataFrame, columns: Dict[str, ColumnAnalysis], query_context: str) -> AggregationStrategy:
        """Analyze if data needs aggregation for meaningful visualization"""
        
        # Check for duplicate categorical values with numeric measures
        categorical_cols = [name for name, analysis in columns.items() if analysis.data_type == DataType.CATEGORICAL]
        numeric_cols = [name for name, analysis in columns.items() if analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]]
        
        if not categorical_cols or not numeric_cols:
            return AggregationStrategy(needs_aggregation=False)
        
        # Check for duplicates in categorical columns
        needs_aggregation = False
        group_by_columns = []
        aggregate_columns = {}
        
        for cat_col in categorical_cols:
            if columns[cat_col].cardinality < len(dataframe):
                # We have duplicate categories - likely needs aggregation
                needs_aggregation = True
                group_by_columns.append(cat_col)
        
        if needs_aggregation:
            # Determine aggregation methods for numeric columns
            for num_col in numeric_cols:
                col_name_lower = num_col.lower()
                
                # Infer aggregation method from column name and query context
                if any(keyword in col_name_lower for keyword in ['count', 'number', 'qty', 'quantity']):
                    aggregate_columns[num_col] = 'sum'
                elif any(keyword in col_name_lower for keyword in ['avg', 'average', 'mean', 'rate', 'score']):
                    aggregate_columns[num_col] = 'mean'
                elif any(keyword in col_name_lower for keyword in ['total', 'sum', 'amount', 'revenue', 'value']):
                    aggregate_columns[num_col] = 'sum'
                elif 'max' in col_name_lower:
                    aggregate_columns[num_col] = 'max'
                elif 'min' in col_name_lower:
                    aggregate_columns[num_col] = 'min'
                else:
                    # Default aggregation based on query context
                    query_lower = query_context.lower()
                    if any(keyword in query_lower for keyword in ['total', 'sum', 'amount']):
                        aggregate_columns[num_col] = 'sum'
                    elif any(keyword in query_lower for keyword in ['average', 'mean']):
                        aggregate_columns[num_col] = 'mean'
                    else:
                        aggregate_columns[num_col] = 'sum'  # Default to sum
        
        reasoning = ""
        if needs_aggregation:
            reasoning = f"Data contains duplicate categories in {', '.join(group_by_columns)}, suggesting aggregation needed for meaningful comparison"
        
        return AggregationStrategy(
            needs_aggregation=needs_aggregation,
            group_by_columns=group_by_columns,
            aggregate_columns=aggregate_columns,
            reasoning=reasoning
        )
    
    def _detect_data_patterns(self, dataframe: pd.DataFrame, columns: Dict[str, ColumnAnalysis]) -> List[str]:
        """Detect common data patterns for better visualization recommendations"""
        patterns = []
        
        # Time series pattern
        temporal_cols = [name for name, analysis in columns.items() if analysis.data_type == DataType.TEMPORAL]
        numeric_cols = [name for name, analysis in columns.items() if analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]]
        
        if temporal_cols and numeric_cols:
            patterns.append("time_series")
        
        # Correlation matrix pattern
        if len(numeric_cols) >= 3 and len(dataframe) <= 10:
            patterns.append("correlation_matrix")
        
        # Summary statistics pattern
        quartile_indicators = ['min', 'max', 'q1', 'q3', 'median', 'quartile']
        quartile_cols = [col for col in dataframe.columns if any(indicator in col.lower() for indicator in quartile_indicators)]
        if len(quartile_cols) >= 3:
            patterns.append("summary_statistics")
        
        # Paired comparison pattern
        if len(dataframe.columns) == 2:
            col_names = [col.lower() for col in dataframe.columns]
            if any(pair in ' '.join(col_names) for pair in ['actual target', 'before after', 'old new']):
                patterns.append("paired_comparison")
        
        # High cardinality categorical pattern
        categorical_cols = [name for name, analysis in columns.items() if analysis.data_type == DataType.CATEGORICAL]
        if categorical_cols and any(columns[col].cardinality > 20 for col in categorical_cols):
            patterns.append("high_cardinality_categorical")
        
        return patterns
    
    def _calculate_complexity_score(self, dataframe: pd.DataFrame, columns: Dict[str, ColumnAnalysis]) -> float:
        """Calculate a complexity score for the dataset"""
        score = 0.0
        
        # Base complexity from column count
        score += len(dataframe.columns) * 0.1
        
        # Add complexity for high cardinality
        for analysis in columns.values():
            if analysis.cardinality > 50:
                score += 0.3
            elif analysis.cardinality > 20:
                score += 0.2
        
        # Add complexity for missing data
        for analysis in columns.values():
            if analysis.null_percentage > 20:
                score += 0.2
        
        # Add complexity for mixed data types
        data_types = set(analysis.data_type for analysis in columns.values())
        if len(data_types) > 2:
            score += 0.2
        
        return min(score, 1.0)  # Cap at 1.0

class VisualizationRuleEngine:
    """Applies data visualization best practices to recommend chart types"""
    
    def __init__(self):
        self.max_pie_categories = 5  # Reduced from 7 per PRD recommendations
        self.max_categorical_items = 20  # Reduced from 50 per PRD recommendations
        self.intent_analyzer = EnhancedIntentAnalyzer()
        
        # Perceptual effectiveness scores for different chart types
        self.perceptual_effectiveness = {
            ChartType.BAR: 0.95,  # Position/length encoding - most accurate
            ChartType.HORIZONTAL_BAR: 0.95,
            ChartType.LINE: 0.9,  # Position encoding for trends
            ChartType.SCATTER: 0.85,  # Position encoding for relationships
            ChartType.AREA: 0.8,  # Area encoding - less precise than position
            ChartType.HISTOGRAM: 0.9,  # Good for distributions
            ChartType.BOX: 0.85,  # Good for outliers and quartiles
            ChartType.PIE: 0.6,  # Angle encoding - least precise
            ChartType.HEATMAP: 0.75,  # Color encoding
            ChartType.TABLE: 0.95,  # Exact values
            ChartType.STACKED_BAR: 0.7,  # Harder to compare segments
            ChartType.STACKED_AREA: 0.65,
            ChartType.TREEMAP: 0.7,
            ChartType.GROUPED_BAR: 0.85
        }
        
    def recommend_charts(self, analysis: DatasetAnalysis, query_context: str = "") -> List[ChartRecommendation]:
        """Returns ranked list of visualization recommendations"""
        
        if analysis.row_count == 0:
            return [self._create_no_data_recommendation()]
        
        # Analyze user intent first
        user_intent = self.intent_analyzer.analyze_user_intent(query_context)
        
        recommendations = []
        
        # Check if query context suggests distribution analysis
        distribution_keywords = ['distribution', 'histogram', 'spread', 'range', 'values', 'frequency']
        is_distribution_query = any(keyword in query_context.lower() for keyword in distribution_keywords)
        
        # Check if query context suggests quartile/box plot analysis
        quartile_keywords = ['quartile', 'quartiles', 'percentile', 'percentiles', 'median', 'iqr', 'interquartile', 'outlier', 'outliers', 'boxplot', 'box plot']
        is_quartile_query = any(keyword in query_context.lower() for keyword in quartile_keywords)
        
        # Check if query context suggests relationship/correlation analysis
        relationship_keywords = ['relationship', 'correlation', 'analyze', 'compare', 'between', 'vs', 'versus', 'against']
        is_relationship_query = any(keyword in query_context.lower() for keyword in relationship_keywords)
        
        # Check if query context suggests composition analysis (NEW)
        composition_keywords = ['composition', 'breakdown', 'distribution by', 'split by', 'proportion', 'percentage', 'share', 'makeup', 'pie', 'portion']
        is_composition_query = any(keyword in query_context.lower() for keyword in composition_keywords)
        
        # Check if query context suggests correlation matrix
        correlation_matrix_keywords = ['correlation matrix', 'correlation between', 'corr matrix', 'correlation table']
        is_correlation_matrix_query = any(keyword in query_context.lower() for keyword in correlation_matrix_keywords)
        
        # Check if query context suggests time series/trend analysis
        time_series_keywords = ['trends over time', 'trend', 'over time', 'time series', 'temporal', 'by day', 'by week', 'by month', 'by year', 'daily', 'weekly', 'monthly', 'yearly']
        is_time_series_query = any(keyword in query_context.lower() for keyword in time_series_keywords)
        
        # Check if data looks like summary statistics (quartile data)
        is_summary_stats = self._detect_summary_statistics(analysis)
        
        # Check if data looks like correlation matrix output
        is_correlation_matrix_data = self._detect_correlation_matrix_data(analysis)
        
        # Single column scenarios
        if analysis.column_count == 1:
            recommendations.extend(self._single_column_recommendations(analysis))
        
        # Two column scenarios  
        elif analysis.column_count == 2:
            # For time series queries with temporal data, prioritize time series analysis
            if is_time_series_query and len(analysis.temporal_columns) >= 1:
                recommendations.extend(self._time_series_focused_recommendations(analysis, query_context))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._two_column_recommendations(analysis))
            # For distribution queries, prioritize single-column analysis even with two columns
            elif is_distribution_query:
                recommendations.extend(self._distribution_focused_recommendations(analysis))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._two_column_recommendations(analysis))
            # For composition queries, prioritize composition analysis
            elif is_composition_query:
                recommendations.extend(self._composition_focused_recommendations(analysis, query_context))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._two_column_recommendations(analysis))
            else:
                # No specialized query detected, use general recommendations
                recommendations.extend(self._two_column_recommendations(analysis))
        
        # Multi-column scenarios
        else:
            # For time series queries, prioritize time series analysis if we have temporal data
            if is_time_series_query and len(analysis.temporal_columns) >= 1:
                recommendations.extend(self._time_series_focused_recommendations(analysis, query_context))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._multi_column_recommendations(analysis))
            # For correlation matrix queries or data, prioritize correlation matrix visualization
            elif is_correlation_matrix_query or is_correlation_matrix_data:
                recommendations.extend(self._correlation_matrix_recommendations(analysis, query_context))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._multi_column_recommendations(analysis))
            # For relationship queries, prioritize relationship analysis even with multiple columns
            elif is_relationship_query and len(analysis.numeric_columns) >= 2:
                recommendations.extend(self._relationship_focused_recommendations(analysis, query_context))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._multi_column_recommendations(analysis))
            # For relationship queries with potential identifier columns, check non-identifier numeric columns
            elif is_relationship_query:
                # Check if we have at least 2 numeric columns when excluding identifiers
                non_id_numeric_cols = [col for col in analysis.numeric_columns 
                                      if not analysis.columns[col].contains_identifiers]
                if len(non_id_numeric_cols) >= 2 or len(analysis.numeric_columns) >= 2:
                    recommendations.extend(self._relationship_focused_recommendations(analysis, query_context))
                    # Only add general recommendations if specialized ones didn't provide enough
                    if len(recommendations) < 3:
                        recommendations.extend(self._multi_column_recommendations(analysis))
                else:
                    # Fall back to multi-column recommendations
                    recommendations.extend(self._multi_column_recommendations(analysis))
            # For quartile queries or summary statistics, prioritize quartile analysis
            elif is_quartile_query or is_summary_stats:
                recommendations.extend(self._quartile_focused_recommendations(analysis, query_context))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._multi_column_recommendations(analysis))
            # For distribution queries, prioritize single-column analysis
            elif is_distribution_query:
                recommendations.extend(self._distribution_focused_recommendations(analysis))
                # Only add general recommendations if specialized ones didn't provide enough
                if len(recommendations) < 3:
                    recommendations.extend(self._multi_column_recommendations(analysis))
            else:
                # No specialized query detected, use general recommendations
                recommendations.extend(self._multi_column_recommendations(analysis))
        
        # Score and rank recommendations with enhanced scoring
        for rec in recommendations:
            rec.confidence_score = self._calculate_score(rec, analysis, query_context)
            rec.perceptual_effectiveness_score = self.perceptual_effectiveness.get(rec.chart_type, 0.7)
            rec.user_intent_match = self._calculate_intent_match(rec, user_intent)
            rec.detailed_explanation = self._generate_detailed_explanation(rec, analysis, user_intent)
            rec.warnings = self._generate_warnings(rec, analysis)
        
        # Remove duplicate chart types, keeping the highest confidence score for each type
        recommendations = self._deduplicate_recommendations(recommendations)
        
        # If no recommendations were generated, provide fallback recommendations
        if not recommendations:
            recommendations = self._generate_fallback_recommendations(analysis, query_context)
        
        recommendations.sort(key=lambda x: x.confidence_score, reverse=True)
        return recommendations[:5]
    
    def _distribution_focused_recommendations(self, analysis: DatasetAnalysis) -> List[ChartRecommendation]:
        """Recommendations focused on distribution analysis for multi-column data"""
        recommendations = []
        
        # Find the most suitable column for distribution analysis
        primary_column = None
        primary_col_analysis = None
        
        # Prioritize columns with "value", "amount", "score", "price" etc. in name
        value_keywords = ['value', 'amount', 'score', 'price', 'cost', 'revenue', 'profit', 'rating']
        for col_name, col_analysis in analysis.columns.items():
            if (col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and
                any(keyword in col_name.lower() for keyword in value_keywords)):
                primary_column = col_name
                primary_col_analysis = col_analysis
                break
        
        # If no value-related column found, use the first numeric column
        if primary_column is None:
            for col_name, col_analysis in analysis.columns.items():
                if col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
                    primary_column = col_name
                    primary_col_analysis = col_analysis
                    break
        
        if primary_column and primary_col_analysis:
            if primary_col_analysis.data_type == DataType.NUMERIC_CONTINUOUS:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.HISTOGRAM,
                    confidence_score=0.95,  # High confidence for distribution queries
                    reasoning=f"Histogram shows distribution of {primary_column} values",
                    x_axis=primary_column,
                    accessibility_notes="Distribution chart with bin counts"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BOX,
                    confidence_score=0.85,
                    reasoning=f"Box plot reveals outliers and quartiles in {primary_column}",
                    y_axis=primary_column,
                    accessibility_notes="Box plot with quartile information"
                ))
        
        return recommendations
    
    def _composition_focused_recommendations(self, analysis: DatasetAnalysis, query_context: str) -> List[ChartRecommendation]:
        """Recommendations focused on composition analysis for categorical breakdown data"""
        recommendations = []
        
        # Find categorical and numeric columns
        categorical_col = None
        numeric_col = None
        categorical_analysis = None
        
        for col_name, col_analysis in analysis.columns.items():
            if col_analysis.data_type == DataType.CATEGORICAL and categorical_col is None:
                categorical_col = col_name
                categorical_analysis = col_analysis
            elif col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and numeric_col is None:
                numeric_col = col_name
        
        # If we have categorical + numeric data suitable for pie chart
        if categorical_col and numeric_col and categorical_analysis:
            if categorical_analysis.cardinality <= self.max_pie_categories:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.PIE,
                    confidence_score=0.95,  # High confidence for composition queries
                    reasoning=f"Pie chart ideal for showing composition of {categorical_analysis.cardinality} {categorical_col} categories",
                    color_by=categorical_col,
                    accessibility_notes="Pie chart showing categorical composition with percentages"
                ))
                
                # Also recommend bar chart as alternative
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.85,
                    reasoning="Bar chart provides alternative view of categorical composition",
                    x_axis=categorical_col,
                    y_axis=numeric_col,
                    accessibility_notes="Bar chart showing categorical composition"
                ))
            else:
                # Too many categories for pie chart, recommend bar chart
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.9,
                    reasoning=f"Bar chart better than pie for {categorical_analysis.cardinality} categories",
                    x_axis=categorical_col,
                    y_axis=numeric_col,
                    accessibility_notes="Bar chart showing categorical composition"
                ))
                
                if categorical_analysis.cardinality > 8:
                    recommendations.append(ChartRecommendation(
                        chart_type=ChartType.HORIZONTAL_BAR,
                        confidence_score=0.85,
                        reasoning="Horizontal bar chart better for many category labels",
                        x_axis=numeric_col,
                        y_axis=categorical_col,
                        accessibility_notes="Horizontal bar chart for categorical composition"
                    ))
        
        return recommendations
    
    def _time_series_focused_recommendations(self, analysis: DatasetAnalysis, query_context: str) -> List[ChartRecommendation]:
        """Recommendations focused on time series/trend analysis for temporal data"""
        recommendations = []
        
        # Find temporal and numeric columns
        temporal_col = None
        numeric_cols = []
        
        for col_name, col_analysis in analysis.columns.items():
            if col_analysis.data_type == DataType.TEMPORAL and temporal_col is None:
                temporal_col = col_name
            elif col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
                numeric_cols.append(col_name)
        
        if temporal_col and numeric_cols:
            # Check for volume/cumulative keywords - prioritize query context over column names
            volume_keywords = ['volume', 'cumulative', 'sum total', 'total volume', 'total amount', 'count']
            is_volume_data = any(keyword in query_context.lower() for keyword in volume_keywords)
            
            # Only check column names if query doesn't explicitly mention trends
            if not is_volume_data and 'trend' not in query_context.lower():
                column_volume_keywords = ['volume', 'cumulative', 'sum', 'count']
                is_volume_data = any(keyword in numeric_cols[0].lower() for keyword in column_volume_keywords)
            
            if is_volume_data:
                # For volume/cumulative data, prefer area chart
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.AREA,
                    confidence_score=0.95,
                    reasoning="Area chart ideal for showing volume/cumulative trends over time",
                    x_axis=temporal_col,
                    y_axis=numeric_cols[0],
                    accessibility_notes="Time series area chart showing cumulative volume trends"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.9,
                    reasoning="Line chart shows trend progression over time",
                    x_axis=temporal_col,
                    y_axis=numeric_cols[0],
                    accessibility_notes="Time series line chart"
                ))
            else:
                # For regular time series, prefer line chart
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.95,
                    reasoning="Line chart ideal for showing trends over time",
                    x_axis=temporal_col,
                    y_axis=numeric_cols[0],
                    accessibility_notes="Time series line chart showing trends"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.AREA,
                    confidence_score=0.85,
                    reasoning="Area chart emphasizes magnitude changes over time",
                    x_axis=temporal_col,
                    y_axis=numeric_cols[0],
                    accessibility_notes="Time series area chart"
                ))
            
            # If we have multiple numeric columns, suggest multiple series line chart
            if len(numeric_cols) > 1:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.9,
                    reasoning=f"Line chart can show multiple trends: {', '.join(numeric_cols[:3])}",
                    x_axis=temporal_col,
                    y_axis=numeric_cols[1],  # Second numeric column
                    accessibility_notes="Multi-series time series line chart"
                ))
        
        return recommendations
    
    def _relationship_focused_recommendations(self, analysis: DatasetAnalysis, query_context: str) -> List[ChartRecommendation]:
        """Recommendations focused on relationship/correlation analysis for multi-column data"""
        recommendations = []
        
        # Find the best numeric columns for relationship analysis
        numeric_cols = analysis.numeric_columns.copy()
        
        # Filter out likely identifier columns from numeric analysis
        filtered_numeric_cols = []
        for col_name in numeric_cols:
            col_analysis = analysis.columns[col_name]
            if not col_analysis.contains_identifiers:
                filtered_numeric_cols.append(col_name)
        
        # If we have at least 2 non-identifier numeric columns, recommend scatter plot
        if len(filtered_numeric_cols) >= 2:
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.SCATTER,
                confidence_score=0.95,  # High confidence for relationship queries
                reasoning=f"Scatter plot reveals relationship between {filtered_numeric_cols[0]} and {filtered_numeric_cols[1]}",
                x_axis=filtered_numeric_cols[0],
                y_axis=filtered_numeric_cols[1],
                accessibility_notes="Scatter plot showing correlation between numeric variables"
            ))
            
            # If we have 3+ numeric columns, also suggest correlation heatmap
            if len(filtered_numeric_cols) >= 3:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.HEATMAP,
                    confidence_score=0.85,
                    reasoning="Heatmap shows correlations between multiple numeric variables",
                    accessibility_notes="Correlation heatmap matrix"
                ))
        
        # Fallback: if no good numeric columns found, use all numeric columns
        elif len(numeric_cols) >= 2:
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.SCATTER,
                confidence_score=0.8,
                reasoning=f"Scatter plot shows relationship between {numeric_cols[0]} and {numeric_cols[1]}",
                x_axis=numeric_cols[0],
                y_axis=numeric_cols[1],
                accessibility_notes="Scatter plot showing correlation"
            ))
        
        # Additional fallback: check if we have columns that could be numeric but were misclassified
        elif len(numeric_cols) < 2 and analysis.column_count >= 2:
            # Look for columns that might be numeric but were misclassified
            potential_numeric_cols = []
            for col_name, col_analysis in analysis.columns.items():
                # Skip obvious identifier columns
                if not col_analysis.contains_identifiers:
                    # Check if column name suggests it's a value/metric column
                    value_keywords = ['value', 'score', 'amount', 'price', 'rating', 'volatility', 'lifetime']
                    if any(keyword in col_name.lower() for keyword in value_keywords):
                        potential_numeric_cols.append(col_name)
                    # Also include already identified numeric columns
                    elif col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
                        potential_numeric_cols.append(col_name)
            
            # If we found at least 2 potential numeric columns, recommend scatter plot
            if len(potential_numeric_cols) >= 2:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.SCATTER,
                    confidence_score=0.85,  # High confidence since this is a relationship query
                    reasoning=f"Scatter plot shows correlation between {potential_numeric_cols[0]} and {potential_numeric_cols[1]}",
                    x_axis=potential_numeric_cols[0],
                    y_axis=potential_numeric_cols[1],
                    accessibility_notes="Scatter plot showing correlation between variables"
                ))
        
        return recommendations
    
    def _detect_summary_statistics(self, analysis: DatasetAnalysis) -> bool:
        """Detect if the data represents summary statistics (like quartiles) rather than raw data"""
        
        # Check for quartile-related column names
        quartile_indicators = [
            'min', 'max', 'q1', 'q3', 'first_quartile', 'third_quartile', 
            'quartile', 'percentile', 'median', 'iqr', 'lower', 'upper'
        ]
        
        column_names = [col.lower() for col in analysis.columns.keys()]
        quartile_columns_found = sum(1 for col in column_names 
                                   if any(indicator in col for indicator in quartile_indicators))
        
        # If we have multiple quartile-related columns, it's likely summary stats
        if quartile_columns_found >= 3:
            return True
        
        # Check if all columns have very low cardinality (typical of summary stats)
        if analysis.column_count >= 3:
            low_cardinality_columns = sum(1 for col_analysis in analysis.columns.values() 
                                        if col_analysis.cardinality <= 5)
            if low_cardinality_columns >= analysis.column_count * 0.8:  # 80% of columns have very low cardinality
                return True
        
        return False
    
    def _detect_correlation_matrix_data(self, analysis: DatasetAnalysis) -> bool:
        """Detect if the data represents correlation matrix output"""
        
        # Check for correlation-related column names
        correlation_indicators = [
            'corr_', 'correlation_', 'corr', 'correlation'
        ]
        
        column_names = [col.lower() for col in analysis.columns.keys()]
        correlation_columns_found = sum(1 for col in column_names 
                                      if any(indicator in col for indicator in correlation_indicators))
        
        # If most columns are correlation-related and we have few rows (typical of correlation matrix output)
        if (correlation_columns_found >= analysis.column_count * 0.5 and  # 50% of columns are correlation-related
            analysis.row_count <= 10 and  # Few rows (typical of correlation matrix summary)
            analysis.column_count >= 3):  # Multiple correlation pairs
            return True
        
        # Check if all columns are numeric with very few rows (typical of correlation matrix summary)
        # This is the pattern we see: 1 row with 6 correlation coefficient columns
        if (analysis.column_count >= 4 and  # Multiple correlation pairs
            len(analysis.numeric_columns) == analysis.column_count and  # All numeric
            analysis.row_count <= 3):  # Very few rows (1-3 rows typical for correlation summary)
            return True
        
        return False
    
    def _correlation_matrix_recommendations(self, analysis: DatasetAnalysis, query_context: str) -> List[ChartRecommendation]:
        """Recommendations for correlation matrix data visualization"""
        recommendations = []
        
        # For correlation matrix data, table view is usually most appropriate
        recommendations.append(ChartRecommendation(
            chart_type=ChartType.TABLE,
            confidence_score=0.95,
            reasoning="Table view ideal for displaying correlation matrix values with precise numbers",
            accessibility_notes="Correlation matrix table with precise correlation coefficients"
        ))
        
        # If we have enough correlation pairs, we could suggest a custom heatmap approach
        # But since the data is already correlation coefficients, standard heatmap won't work well
        if analysis.column_count >= 4:
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.TABLE,
                confidence_score=0.9,
                reasoning="Detailed table view shows all correlation coefficients clearly",
                accessibility_notes="Comprehensive correlation matrix table"
            ))
        
        # Bar chart could show correlation strengths if we transform the data
        recommendations.append(ChartRecommendation(
            chart_type=ChartType.BAR,
            confidence_score=0.7,
            reasoning="Bar chart could show correlation strengths (requires data transformation)",
            accessibility_notes="Bar chart showing correlation coefficient magnitudes"
        ))
        
        return recommendations
    
    def _quartile_focused_recommendations(self, analysis: DatasetAnalysis, query_context: str) -> List[ChartRecommendation]:
        """Recommendations focused on quartile/box plot analysis for summary statistics data"""
        recommendations = []
        
        # For quartile data, box plot is usually most appropriate
        # Try to find the main value column or use the first numeric column
        primary_column = None
        
        # Look for columns that might represent the main values
        value_indicators = ['sentiment', 'score', 'value', 'amount', 'price', 'rating']
        for col_name, col_analysis in analysis.columns.items():
            if (col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and
                any(indicator in col_name.lower() for indicator in value_indicators)):
                primary_column = col_name
                break
        
        # If no obvious value column, use first numeric column
        if primary_column is None:
            for col_name, col_analysis in analysis.columns.items():
                if col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
                    primary_column = col_name
                    break
        
        if primary_column:
            # Box plot is ideal for quartile analysis
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.BOX,
                confidence_score=0.95,  # High confidence for quartile queries
                reasoning=f"Box plot ideal for quartile analysis and outlier detection of {primary_column}",
                y_axis=primary_column,
                accessibility_notes="Box plot showing quartiles, median, and outliers"
            ))
            
            # Table view is also useful for summary statistics
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.TABLE,
                confidence_score=0.85,
                reasoning="Table view shows detailed quartile statistics",
                accessibility_notes="Detailed summary statistics table"
            ))
            
            # Histogram is less appropriate for summary stats but can be included with lower confidence
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.HISTOGRAM,
                confidence_score=0.4,  # Lower confidence for summary stats
                reasoning=f"Histogram less suitable for summary statistics of {primary_column}",
                x_axis=primary_column,
                accessibility_notes="Distribution chart with bin counts"
            ))
        
        return recommendations
    
    def _single_column_recommendations(self, analysis: DatasetAnalysis) -> List[ChartRecommendation]:
        """Recommendations for single column data"""
        recommendations = []
        col_name = list(analysis.columns.keys())[0]
        col_analysis = analysis.columns[col_name]
        
        if col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.HISTOGRAM,
                confidence_score=0.95,  # Higher confidence for numeric distribution
                reasoning="Histogram shows distribution of numeric data",
                x_axis=col_name,
                accessibility_notes="Distribution chart with bin counts"
            ))
            
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.BOX,
                confidence_score=0.75,
                reasoning="Box plot reveals outliers and quartiles",
                y_axis=col_name,
                accessibility_notes="Box plot with quartile information"
            ))
        
        elif col_analysis.data_type == DataType.CATEGORICAL:
            if col_analysis.cardinality <= self.max_pie_categories:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.PIE,
                    confidence_score=0.8,
                    reasoning=f"Pie chart effective for {col_analysis.cardinality} categories",
                    color_by=col_name,
                    accessibility_notes="Pie chart with category percentages"
                ))
            
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.BAR,
                confidence_score=0.9,
                reasoning="Bar chart shows frequency of categories",
                x_axis=col_name,
                aggregation_method="count",
                accessibility_notes="Frequency bar chart"
            ))
        
        return recommendations
    
    def _two_column_recommendations(self, analysis: DatasetAnalysis) -> List[ChartRecommendation]:
        """Recommendations for two column data"""
        recommendations = []
        cols = list(analysis.columns.keys())
        col1_analysis = analysis.columns[cols[0]]
        col2_analysis = analysis.columns[cols[1]]
        
        # Check if this looks like value + frequency data (common for distribution queries)
        value_keywords = ['value', 'price', 'amount', 'cost', 'revenue', 'profit', 'ltv', 'clv', 'score', 'rating']
        count_keywords = ['count', 'frequency', 'number', 'total']
        
        col1_is_value = any(keyword in cols[0].lower() for keyword in value_keywords)
        col2_is_count = any(keyword in cols[1].lower() for keyword in count_keywords)
        col2_is_value = any(keyword in cols[1].lower() for keyword in value_keywords)
        col1_is_count = any(keyword in cols[0].lower() for keyword in count_keywords)
        
        # Value + Count pattern: Suggest histogram for distribution analysis
        if ((col1_is_value and col2_is_count) or (col2_is_value and col1_is_count)) and \
           (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
            col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
            
            value_col = cols[0] if col1_is_value else cols[1]
            count_col = cols[1] if col1_is_value else cols[0]
            
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.HISTOGRAM,
                confidence_score=0.9,
                reasoning=f"Histogram shows distribution of {value_col} values",
                x_axis=value_col,
                y_axis=count_col,
                accessibility_notes="Distribution histogram with frequency counts"
            ))
            
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.BAR,
                confidence_score=0.85,
                reasoning="Bar chart shows frequency across value ranges",
                x_axis=value_col,
                y_axis=count_col,
                accessibility_notes="Frequency bar chart"
            ))
        
        # Temporal + Numeric: Line chart (but consider area for volume/cumulative data)
        elif (col1_analysis.data_type == DataType.TEMPORAL and 
              col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
            
            # Check if the numeric column suggests volume/cumulative data
            volume_keywords = ['volume', 'total', 'cumulative', 'sum', 'count', 'amount']
            is_volume_data = any(keyword in cols[1].lower() for keyword in volume_keywords)
            
            if is_volume_data:
                # For volume/cumulative data, prefer area chart
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.AREA,
                    confidence_score=0.95,
                    reasoning="Area chart ideal for cumulative/volume data over time",
                    x_axis=cols[0],
                    y_axis=cols[1],
                    accessibility_notes="Time series area chart showing cumulative volume"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.9,
                    reasoning="Line chart shows trend over time",
                    x_axis=cols[0],
                    y_axis=cols[1],
                    accessibility_notes="Time series line chart"
                ))
            else:
                # For regular time series, prefer line chart
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.95,
                    reasoning="Line chart ideal for time series data",
                    x_axis=cols[0],
                    y_axis=cols[1],
                    accessibility_notes="Time series line chart"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.AREA,
                    confidence_score=0.8,
                    reasoning="Area chart emphasizes magnitude over time",
                    x_axis=cols[0],
                    y_axis=cols[1],
                    accessibility_notes="Time series area chart"
                ))
        
        # Numeric + Temporal: Handle reverse temporal order
        elif (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
              col2_analysis.data_type == DataType.TEMPORAL):
            
            # Check if the numeric column suggests volume/cumulative data
            volume_keywords = ['volume', 'total', 'cumulative', 'sum', 'count', 'amount']
            is_volume_data = any(keyword in cols[0].lower() for keyword in volume_keywords)
            
            if is_volume_data:
                # For volume/cumulative data, prefer area chart
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.AREA,
                    confidence_score=0.95,
                    reasoning="Area chart ideal for cumulative/volume data over time",
                    x_axis=cols[1],  # Time column as X-axis
                    y_axis=cols[0],  # Value column as Y-axis
                    accessibility_notes="Time series area chart showing cumulative volume"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.9,
                    reasoning="Line chart shows trend over time",
                    x_axis=cols[1],  # Time column as X-axis
                    y_axis=cols[0],  # Value column as Y-axis
                    accessibility_notes="Time series line chart"
                ))
            else:
                # For regular time series, prefer line chart
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.95,
                    reasoning="Line chart ideal for time series data",
                    x_axis=cols[1],  # Time column as X-axis
                    y_axis=cols[0],  # Value column as Y-axis
                    accessibility_notes="Time series line chart"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.AREA,
                    confidence_score=0.8,
                    reasoning="Area chart emphasizes magnitude over time",
                    x_axis=cols[1],  # Time column as X-axis
                    y_axis=cols[0],  # Value column as Y-axis
                    accessibility_notes="Time series area chart"
                ))
        
        # Categorical + Numeric: Bar chart and potentially pie chart
        elif (col1_analysis.data_type == DataType.CATEGORICAL and 
              col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
            
            # Always recommend bar chart for categorical comparisons, but adjust based on cardinality
            if col1_analysis.cardinality <= self.max_categorical_items:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.9,
                    reasoning="Bar chart compares values across categories",
                    x_axis=cols[0],
                    y_axis=cols[1],
                    accessibility_notes="Categorical comparison bar chart"
                ))
                
                # Add pie chart recommendation for suitable categorical data
                if col1_analysis.cardinality <= self.max_pie_categories:
                    recommendations.append(ChartRecommendation(
                        chart_type=ChartType.PIE,
                        confidence_score=0.8,
                        reasoning=f"Pie chart effective for composition of {col1_analysis.cardinality} categories",
                        color_by=cols[0],
                        accessibility_notes="Pie chart showing categorical composition with percentages"
                    ))
                
                if col1_analysis.cardinality > 8:
                    recommendations.append(ChartRecommendation(
                        chart_type=ChartType.HORIZONTAL_BAR,
                        confidence_score=0.85,
                        reasoning="Horizontal bar better for many categories",
                        x_axis=cols[1],
                        y_axis=cols[0],
                        accessibility_notes="Horizontal categorical comparison chart"
                    ))
            else:
                # For high cardinality categorical data, still recommend charts but with lower confidence
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.HORIZONTAL_BAR,
                    confidence_score=0.7,
                    reasoning=f"Horizontal bar chart for {col1_analysis.cardinality} categories (consider filtering)",
                    x_axis=cols[1],
                    y_axis=cols[0],
                    accessibility_notes="Horizontal categorical comparison chart with many categories"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.TABLE,
                    confidence_score=0.8,
                    reasoning="Table view recommended for high cardinality categorical data",
                    accessibility_notes="Detailed data table"
                ))
        
        # Numeric + Categorical: Handle reverse order
        elif (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
              col2_analysis.data_type == DataType.CATEGORICAL):
            
            # Always recommend bar chart for categorical comparisons, but adjust based on cardinality
            if col2_analysis.cardinality <= self.max_categorical_items:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.9,
                    reasoning="Bar chart compares values across categories",
                    x_axis=cols[1],  # Categorical column as X-axis
                    y_axis=cols[0],  # Numeric column as Y-axis
                    accessibility_notes="Categorical comparison bar chart"
                ))
                
                # Add pie chart recommendation for suitable categorical data
                if col2_analysis.cardinality <= self.max_pie_categories:
                    recommendations.append(ChartRecommendation(
                        chart_type=ChartType.PIE,
                        confidence_score=0.8,
                        reasoning=f"Pie chart effective for composition of {col2_analysis.cardinality} categories",
                        color_by=cols[1],
                        accessibility_notes="Pie chart showing categorical composition with percentages"
                    ))
                
                if col2_analysis.cardinality > 8:
                    recommendations.append(ChartRecommendation(
                        chart_type=ChartType.HORIZONTAL_BAR,
                        confidence_score=0.85,
                        reasoning="Horizontal bar better for many categories",
                        x_axis=cols[0],  # Numeric column as X-axis
                        y_axis=cols[1],  # Categorical column as Y-axis
                        accessibility_notes="Horizontal categorical comparison chart"
                    ))
            else:
                # For high cardinality categorical data, still recommend charts but with lower confidence
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.HORIZONTAL_BAR,
                    confidence_score=0.7,
                    reasoning=f"Horizontal bar chart for {col2_analysis.cardinality} categories (consider filtering)",
                    x_axis=cols[0],  # Numeric column as X-axis
                    y_axis=cols[1],  # Categorical column as Y-axis
                    accessibility_notes="Horizontal categorical comparison chart with many categories"
                ))
                
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.TABLE,
                    confidence_score=0.8,
                    reasoning="Table view recommended for high cardinality categorical data",
                    accessibility_notes="Detailed data table"
                ))
        
        # Numeric + Numeric: Scatter plot
        elif (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
              col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.SCATTER,
                confidence_score=0.9,
                reasoning="Scatter plot reveals relationships between numeric variables",
                x_axis=cols[0],
                y_axis=cols[1],
                accessibility_notes="Scatter plot showing correlation"
            ))
        
        return recommendations
    
    def _multi_column_recommendations(self, analysis: DatasetAnalysis) -> List[ChartRecommendation]:
        """Recommendations for multi-column data"""
        recommendations = []
        
        # Filter out identifier columns for visualization purposes
        non_id_columns = {name: col_analysis for name, col_analysis in analysis.columns.items() 
                         if not col_analysis.contains_identifiers}
        
        # Debug: Check what columns are being identified
        # This will help us understand why the logic isn't working
        id_columns = [name for name, col_analysis in analysis.columns.items() if col_analysis.contains_identifiers]
        
        # If we have exactly 2 non-identifier columns that are both numeric, treat as relationship analysis
        if len(non_id_columns) == 2:
            non_id_cols = list(non_id_columns.keys())
            col1_analysis = non_id_columns[non_id_cols[0]]
            col2_analysis = non_id_columns[non_id_cols[1]]
            
            # Two numeric columns: Scatter plot for relationship analysis
            if (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
                col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.SCATTER,
                    confidence_score=0.9,
                    reasoning=f"Scatter plot reveals relationship between {non_id_cols[0]} and {non_id_cols[1]}",
                    x_axis=non_id_cols[0],
                    y_axis=non_id_cols[1],
                    accessibility_notes="Scatter plot showing correlation between numeric variables"
                ))
            
            # Temporal + Numeric: Line chart
            elif (col1_analysis.data_type == DataType.TEMPORAL and 
                  col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.9,
                    reasoning="Line chart ideal for time series data",
                    x_axis=non_id_cols[0],
                    y_axis=non_id_cols[1],
                    accessibility_notes="Time series line chart"
                ))
            
            # Numeric + Temporal: Handle reverse order
            elif (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
                  col2_analysis.data_type == DataType.TEMPORAL):
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.LINE,
                    confidence_score=0.9,
                    reasoning="Line chart ideal for time series data",
                    x_axis=non_id_cols[1],  # Temporal column as X
                    y_axis=non_id_cols[0],  # Numeric column as Y
                    accessibility_notes="Time series line chart"
                ))
            
            # Categorical + Numeric: Bar chart
            elif (col1_analysis.data_type == DataType.CATEGORICAL and 
                  col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.9,
                    reasoning="Bar chart compares values across categories",
                    x_axis=non_id_cols[0],
                    y_axis=non_id_cols[1],
                    accessibility_notes="Categorical comparison bar chart"
                ))
            
            # Numeric + Categorical: Handle reverse order
            elif (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
                  col2_analysis.data_type == DataType.CATEGORICAL):
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.9,
                    reasoning="Bar chart compares values across categories",
                    x_axis=non_id_cols[1],  # Categorical column as X
                    y_axis=non_id_cols[0],  # Numeric column as Y
                    accessibility_notes="Categorical comparison bar chart"
                ))
        
        # If we have multiple numeric columns (3+), suggest correlation analysis
        elif len(analysis.numeric_columns) >= 3:
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.HEATMAP,
                confidence_score=0.8,
                reasoning="Heatmap shows correlations between multiple numeric variables",
                accessibility_notes="Correlation heatmap"
            ))
            
            # Also suggest scatter plot for first two numeric columns
            if len(analysis.numeric_columns) >= 2:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.SCATTER,
                    confidence_score=0.7,
                    reasoning=f"Scatter plot shows relationship between {analysis.numeric_columns[0]} and {analysis.numeric_columns[1]}",
                    x_axis=analysis.numeric_columns[0],
                    y_axis=analysis.numeric_columns[1],
                    accessibility_notes="Scatter plot showing correlation"
                ))
        
        # If we have temporal + multiple numeric columns
        elif analysis.temporal_columns and len(analysis.numeric_columns) >= 1:
            recommendations.append(ChartRecommendation(
                chart_type=ChartType.LINE,
                confidence_score=0.85,
                reasoning="Line chart for trends over time",
                x_axis=analysis.temporal_columns[0],
                y_axis=analysis.numeric_columns[0],
                accessibility_notes="Time trend chart"
            ))
        
        # If we have categorical + numeric columns
        elif analysis.categorical_columns and analysis.numeric_columns:
            # Filter out identifier columns from categorical
            non_id_categorical = [col for col in analysis.categorical_columns 
                                if not analysis.columns[col].contains_identifiers]
            
            if non_id_categorical:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.8,
                    reasoning="Bar chart compares metrics across categories", 
                    x_axis=non_id_categorical[0],
                    y_axis=analysis.numeric_columns[0],
                    accessibility_notes="Categorical comparison chart"
                ))
        
        # Table view as fallback (lower priority now)
        recommendations.append(ChartRecommendation(
            chart_type=ChartType.TABLE,
            confidence_score=0.6,
            reasoning="Table provides detailed view of multi-dimensional data",
            accessibility_notes="Detailed data table"
        ))
        
        return recommendations
    
    def _calculate_score(self, recommendation: ChartRecommendation, analysis: DatasetAnalysis, query_context: str = "") -> float:
        """Calculate confidence score for a recommendation"""
        base_score = recommendation.confidence_score
        
        # Adjust based on data characteristics
        if recommendation.chart_type == ChartType.SCATTER and analysis.row_count > 10000:
            base_score *= 0.8  # Performance penalty for large scatter plots
        
        if recommendation.chart_type == ChartType.PIE and analysis.row_count > 1000:
            base_score *= 0.9  # Pie charts less effective with many data points
        
        # Check if this looks like correlation matrix data regardless of query context
        is_correlation_data = self._detect_correlation_matrix_data(analysis)
        if is_correlation_data:
            if recommendation.chart_type == ChartType.TABLE:
                base_score *= 1.8  # Strong boost for table view for correlation data
            elif recommendation.chart_type == ChartType.HEATMAP:
                base_score *= 0.2  # Reduce heatmap for correlation coefficient data
            elif recommendation.chart_type == ChartType.SCATTER:
                base_score *= 0.2  # Reduce scatter plot for correlation coefficient data
        
        # Boost scores based on query context
        if query_context:
            query_lower = query_context.lower()
            
            # Distribution-related queries (highest priority for histogram)
            distribution_keywords = ['distribution', 'histogram', 'spread', 'range', 'frequency']
            is_distribution_query = any(keyword in query_lower for keyword in distribution_keywords)
            
            if is_distribution_query:
                if recommendation.chart_type == ChartType.HISTOGRAM:
                    base_score *= 1.5  # Strong boost for histogram in distribution queries
                elif recommendation.chart_type == ChartType.BOX:
                    base_score *= 1.2  # Moderate boost for box plot in distribution queries
                elif recommendation.chart_type == ChartType.LINE:
                    base_score *= 0.5  # Significantly reduce line chart for distribution queries
                elif recommendation.chart_type == ChartType.AREA:
                    base_score *= 0.5  # Significantly reduce area chart for distribution queries
                elif recommendation.chart_type == ChartType.SCATTER:
                    base_score *= 0.6  # Reduce scatter plot for distribution queries
            
            # Quartile-related queries (but not distribution queries)
            elif any(keyword in query_lower for keyword in ['quartile', 'quartiles', 'percentile', 'percentiles', 'median', 'iqr', 'interquartile', 'outlier', 'outliers', 'boxplot', 'box plot']):
                if recommendation.chart_type == ChartType.BOX:
                    base_score *= 1.4  # Strong boost for box plot in quartile queries
                elif recommendation.chart_type == ChartType.HISTOGRAM:
                    base_score *= 0.8  # Slight reduction for histogram in quartile queries
                elif recommendation.chart_type == ChartType.TABLE:
                    base_score *= 1.2  # Boost table for detailed quartile stats
            
            # Cumulative/Volume-related queries (prioritize area charts)
            elif any(word in query_lower for word in ['cumulative', 'volume', 'total over time', 'accumulation', 'buildup', 'running total']):
                if recommendation.chart_type == ChartType.AREA:
                    base_score *= 1.4  # Strong boost for area chart for cumulative/volume queries
                elif recommendation.chart_type == ChartType.LINE:
                    base_score *= 1.1  # Moderate boost for line chart for cumulative queries
                elif recommendation.chart_type == ChartType.HISTOGRAM:
                    base_score *= 0.5  # Reduce histogram for cumulative queries
            
            # Time series/Trend-related queries (but not cumulative)
            elif any(word in query_lower for word in ['trends over time', 'trend', 'over time', 'time series', 'temporal', 'by day', 'by week', 'by month', 'by year', 'daily', 'weekly', 'monthly', 'yearly']):
                if recommendation.chart_type == ChartType.LINE:
                    base_score *= 1.5  # Strong boost for line chart for time series queries
                elif recommendation.chart_type == ChartType.AREA:
                    base_score *= 1.2  # Boost area chart for trend queries
                elif recommendation.chart_type == ChartType.SCATTER:
                    base_score *= 0.3  # Drastically reduce scatter plot for time series queries
                elif recommendation.chart_type == ChartType.HISTOGRAM:
                    base_score *= 0.6  # Reduce histogram for trend queries
            
            # Comparison-related queries
            elif any(word in query_lower for word in ['compare', 'comparison', 'versus', 'vs', 'by category']):
                if recommendation.chart_type == ChartType.BAR:
                    base_score *= 1.2  # Boost bar chart for comparison queries
                elif recommendation.chart_type == ChartType.HORIZONTAL_BAR:
                    base_score *= 1.1  # Boost horizontal bar for comparison queries
            
            # Correlation/Relationship-related queries
            elif any(word in query_lower for word in ['correlation', 'relationship', 'scatter', 'vs', 'analyze', 'between']):
                if recommendation.chart_type == ChartType.SCATTER:
                    base_score *= 1.3  # Strong boost for scatter plot for correlation/relationship queries
                    # Extra boost for specific value-related terms
                    value_terms = ['volatility', 'lifetime', 'customer lifetime value', 'clv', 'ltv']
                    if any(term in query_lower for term in value_terms):
                        base_score *= 1.2  # Additional boost for value-focused correlation queries
                elif recommendation.chart_type == ChartType.HEATMAP:
                    base_score *= 1.2  # Boost heatmap for multi-variable correlation
                elif recommendation.chart_type == ChartType.TABLE:
                    base_score *= 0.7  # Reduce table priority for relationship queries
                elif recommendation.chart_type == ChartType.BAR:
                    base_score *= 0.5  # Significantly reduce bar chart for relationship queries
            
            # Composition-related queries
            elif any(word in query_lower for word in ['composition', 'breakdown', 'distribution by', 'split by', 'proportion', 'percentage', 'share', 'makeup', 'pie', 'portion']):
                if recommendation.chart_type == ChartType.PIE:
                    base_score *= 1.4  # Strong boost for pie chart for composition queries
                elif recommendation.chart_type == ChartType.BAR:
                    base_score *= 1.1  # Moderate boost for bar chart as alternative
                elif recommendation.chart_type == ChartType.HORIZONTAL_BAR:
                    base_score *= 1.05  # Slight boost for horizontal bar chart
                elif recommendation.chart_type == ChartType.HISTOGRAM:
                    base_score *= 0.6  # Reduce histogram for composition queries
                elif recommendation.chart_type == ChartType.SCATTER:
                    base_score *= 0.5  # Reduce scatter plot for composition queries
            
            # Correlation matrix-related queries
            elif any(word in query_lower for word in ['correlation matrix', 'correlation between', 'corr matrix', 'correlation table']):
                if recommendation.chart_type == ChartType.TABLE:
                    base_score *= 2.0  # Very strong boost for table view for correlation matrix
                elif recommendation.chart_type == ChartType.BAR:
                    base_score *= 1.2  # Moderate boost for bar chart (with transformation)
                elif recommendation.chart_type == ChartType.HEATMAP:
                    base_score *= 0.1  # Drastically reduce heatmap for correlation matrix data
                elif recommendation.chart_type == ChartType.SCATTER:
                    base_score *= 0.1  # Drastically reduce scatter plot for correlation matrix data
        
        return min(1.0, base_score)
    
    def _generate_fallback_recommendations(self, analysis: DatasetAnalysis, query_context: str = "") -> List[ChartRecommendation]:
        """Generate fallback recommendations when primary logic fails"""
        recommendations = []
        
        # Always provide table as a fallback
        recommendations.append(ChartRecommendation(
            chart_type=ChartType.TABLE,
            confidence_score=0.6,
            reasoning="Table view provides detailed data when other visualizations are not suitable",
            accessibility_notes="Detailed data table"
        ))
        
        # For 2 columns, try to provide basic recommendations based on simple heuristics
        if analysis.column_count == 2:
            cols = list(analysis.columns.keys())
            col1_analysis = analysis.columns[cols[0]]
            col2_analysis = analysis.columns[cols[1]]
            
            # If we have any categorical and numeric combination, recommend bar chart
            if ((col1_analysis.data_type == DataType.CATEGORICAL and 
                 col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]) or
                (col2_analysis.data_type == DataType.CATEGORICAL and 
                 col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE])):
                
                # Determine which is categorical and which is numeric
                if col1_analysis.data_type == DataType.CATEGORICAL:
                    cat_col, num_col = cols[0], cols[1]
                    cat_analysis = col1_analysis
                else:
                    cat_col, num_col = cols[1], cols[0]
                    cat_analysis = col2_analysis
                
                # Always recommend bar chart for categorical comparisons
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.BAR,
                    confidence_score=0.8,
                    reasoning=f"Bar chart compares {num_col} across {cat_analysis.cardinality} {cat_col} categories",
                    x_axis=cat_col,
                    y_axis=num_col,
                    accessibility_notes="Categorical comparison bar chart"
                ))
                
                # Add horizontal bar if many categories
                if cat_analysis.cardinality > 8:
                    recommendations.append(ChartRecommendation(
                        chart_type=ChartType.HORIZONTAL_BAR,
                        confidence_score=0.75,
                        reasoning="Horizontal bar chart better for category labels",
                        x_axis=num_col,
                        y_axis=cat_col,
                        accessibility_notes="Horizontal categorical comparison chart"
                    ))
            
            # For two numeric columns, suggest scatter plot
            elif (col1_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE] and 
                  col2_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]):
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.SCATTER,
                    confidence_score=0.7,
                    reasoning="Scatter plot shows relationship between numeric variables",
                    x_axis=cols[0],
                    y_axis=cols[1],
                    accessibility_notes="Scatter plot showing correlation"
                ))
        
        # For single numeric column, suggest histogram
        elif analysis.column_count == 1:
            col_name = list(analysis.columns.keys())[0]
            col_analysis = analysis.columns[col_name]
            
            if col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]:
                recommendations.append(ChartRecommendation(
                    chart_type=ChartType.HISTOGRAM,
                    confidence_score=0.8,
                    reasoning="Histogram shows distribution of numeric data",
                    x_axis=col_name,
                    accessibility_notes="Distribution histogram"
                ))
        
        return recommendations
    
    def _deduplicate_recommendations(self, recommendations: List[ChartRecommendation]) -> List[ChartRecommendation]:
        """Remove duplicate chart types, keeping the highest confidence score for each type"""
        if not recommendations:
            return recommendations
        
        # Dictionary to track the best recommendation for each chart type
        best_by_type = {}
        
        for rec in recommendations:
            chart_type = rec.chart_type
            
            # If we haven't seen this chart type before, or this one has higher confidence
            if (chart_type not in best_by_type or 
                rec.confidence_score > best_by_type[chart_type].confidence_score):
                best_by_type[chart_type] = rec
        
        # Return the deduplicated list
        return list(best_by_type.values())
    
    def _create_no_data_recommendation(self) -> ChartRecommendation:
        """Create recommendation for empty datasets"""
        return ChartRecommendation(
            chart_type=ChartType.TABLE,
            confidence_score=1.0,
            reasoning="No data available to visualize",
            accessibility_notes="Empty dataset notification"
        )
    
    def _calculate_intent_match(self, recommendation: ChartRecommendation, user_intent: UserIntent) -> float:
        """Calculate how well a chart recommendation matches user intent"""
        if user_intent.confidence < 0.3:
            return 0.5  # Low confidence in intent detection
        
        chart_type = recommendation.chart_type
        primary_intent = user_intent.primary_intent
        
        # Define intent-chart matching scores
        intent_chart_scores = {
            'distribution': {
                ChartType.HISTOGRAM: 1.0,
                ChartType.BOX: 0.9,
                ChartType.BAR: 0.6,
                ChartType.TABLE: 0.7
            },
            'comparison': {
                ChartType.BAR: 1.0,
                ChartType.HORIZONTAL_BAR: 0.95,
                ChartType.GROUPED_BAR: 0.9,
                ChartType.TABLE: 0.8
            },
            'trend': {
                ChartType.LINE: 1.0,
                ChartType.AREA: 0.9,
                ChartType.STACKED_AREA: 0.8,
                ChartType.BAR: 0.6
            },
            'composition': {
                ChartType.PIE: 0.9,  # Reduced from 1.0 due to perceptual limitations
                ChartType.STACKED_BAR: 0.95,
                ChartType.TREEMAP: 0.85,
                ChartType.BAR: 0.8
            },
            'relationship': {
                ChartType.SCATTER: 1.0,
                ChartType.HEATMAP: 0.85,
                ChartType.TABLE: 0.6
            },
            'quartile': {
                ChartType.BOX: 1.0,
                ChartType.TABLE: 0.8,
                ChartType.HISTOGRAM: 0.6
            }
        }
        
        # Get base score for intent-chart match
        base_score = intent_chart_scores.get(primary_intent, {}).get(chart_type, 0.5)
        
        # Boost for secondary intents
        for secondary_intent in user_intent.secondary_intents:
            secondary_score = intent_chart_scores.get(secondary_intent, {}).get(chart_type, 0.0)
            base_score += secondary_score * 0.3  # Weight secondary intents less
        
        # Apply user intent confidence
        final_score = base_score * user_intent.confidence
        
        return min(final_score, 1.0)
    
    def _generate_detailed_explanation(self, recommendation: ChartRecommendation, analysis: DatasetAnalysis, user_intent: UserIntent) -> str:
        """Generate detailed, user-friendly explanation for chart recommendation"""
        chart_type = recommendation.chart_type.value.replace('_', ' ').title()
        
        # Base explanation templates
        explanations = {
            ChartType.BAR: f"A Bar Chart is recommended because your data compares values across categories. With {analysis.row_count:,} data points, this chart clearly shows differences between groups, making it easy to identify which categories have higher or lower values.",
            
            ChartType.LINE: f"A Line Chart is ideal for your data because it shows trends over time. This visualization connects data points to reveal patterns, changes, and trends in your {analysis.row_count:,} records, making it perfect for tracking progress or identifying patterns.",
            
            ChartType.SCATTER: f"A Scatter Plot is recommended to reveal relationships between your numeric variables. With {analysis.row_count:,} data points, this chart helps identify correlations, clusters, or outliers that might not be obvious in other visualizations.",
            
            ChartType.PIE: f"A Pie Chart shows the composition of your data as parts of a whole. Since you have a manageable number of categories, this visualization makes it easy to see proportions and percentages at a glance.",
            
            ChartType.HISTOGRAM: f"A Histogram reveals the distribution pattern in your numeric data. This chart groups your {analysis.row_count:,} values into ranges, showing where most values fall and helping identify outliers or unusual patterns.",
            
            ChartType.BOX: f"A Box Plot provides a statistical summary of your data, showing quartiles, median, and outliers. This is particularly useful for understanding the spread and identifying unusual values in your dataset.",
            
            ChartType.HEATMAP: f"A Heatmap visualizes relationships between multiple variables using color intensity. This is effective for showing correlation patterns or comparing values across two dimensions.",
            
            ChartType.TABLE: f"A Table view provides precise values and detailed information. With {analysis.row_count:,} rows and {analysis.column_count} columns, this format ensures you can see exact numbers and perform detailed analysis."
        }
        
        base_explanation = explanations.get(recommendation.chart_type, f"This {chart_type} is recommended based on your data characteristics.")
        
        # Add intent-specific context
        if user_intent.primary_intent == 'distribution':
            base_explanation += " This addresses your interest in understanding how values are distributed across the range."
        elif user_intent.primary_intent == 'comparison':
            base_explanation += " This directly supports your goal of comparing different groups or categories."
        elif user_intent.primary_intent == 'trend':
            base_explanation += " This visualization effectively shows the trends and changes you're looking to analyze."
        elif user_intent.primary_intent == 'relationship':
            base_explanation += " This chart type excels at revealing the relationships and correlations you want to explore."
        
        # Add data-specific insights
        if analysis.needs_aggregation:
            base_explanation += f" Note: Your data contains duplicate categories, so we'll aggregate values by {', '.join(analysis.aggregation_suggestions.keys())} for meaningful comparison."
        
        if analysis.complexity_score > 0.7:
            base_explanation += " Given the complexity of your dataset, this visualization provides the clearest way to understand your data patterns."
        
        return base_explanation
    
    def _generate_warnings(self, recommendation: ChartRecommendation, analysis: DatasetAnalysis) -> List[str]:
        """Generate warnings about potential chart limitations or issues"""
        warnings = []
        
        chart_type = recommendation.chart_type
        
        # Pie chart warnings
        if chart_type == ChartType.PIE:
            categorical_cols = [col for col, col_analysis in analysis.columns.items() 
                              if col_analysis.data_type == DataType.CATEGORICAL]
            if categorical_cols:
                max_categories = max(analysis.columns[col].cardinality for col in categorical_cols)
                if max_categories > self.max_pie_categories:
                    warnings.append(f"Pie charts become hard to read with more than {self.max_pie_categories} categories. Consider using a bar chart instead.")
                
                # Check for small slice sizes
                if analysis.row_count > 100:
                    warnings.append("Some pie slices may be too small to distinguish clearly. Bar charts provide better precision for detailed comparisons.")
        
        # Scatter plot warnings
        elif chart_type == ChartType.SCATTER:
            if analysis.row_count > 5000:
                warnings.append("Large datasets may cause overplotting in scatter plots. Consider sampling or using density plots for clearer patterns.")
            
            # Check for discrete numeric variables
            numeric_cols = [col for col, col_analysis in analysis.columns.items() 
                           if col_analysis.data_type in [DataType.NUMERIC_CONTINUOUS, DataType.NUMERIC_DISCRETE]]
            discrete_cols = [col for col in numeric_cols 
                           if analysis.columns[col].data_type == DataType.NUMERIC_DISCRETE]
            if len(discrete_cols) >= 2:
                warnings.append("Your data contains discrete numeric values which may overlap in scatter plots. Consider adding jitter or using grouped charts.")
        
        # Bar chart warnings
        elif chart_type in [ChartType.BAR, ChartType.HORIZONTAL_BAR]:
            categorical_cols = [col for col, col_analysis in analysis.columns.items() 
                              if col_analysis.data_type == DataType.CATEGORICAL]
            if categorical_cols:
                max_categories = max(analysis.columns[col].cardinality for col in categorical_cols)
                if max_categories > self.max_categorical_items:
                    warnings.append(f"Too many categories ({max_categories}) may make the chart cluttered. Consider filtering to show only the top categories.")
        
        # Heatmap warnings
        elif chart_type == ChartType.HEATMAP:
            if analysis.column_count > 10:
                warnings.append("Large correlation matrices can be overwhelming. Consider focusing on key variables of interest.")
        
        # General data quality warnings
        high_null_cols = [col for col, col_analysis in analysis.columns.items() 
                         if col_analysis.null_percentage > 20]
        if high_null_cols:
            warnings.append(f"Some columns have significant missing data: {', '.join(high_null_cols)}. This may affect visualization accuracy.")
        
        return warnings
