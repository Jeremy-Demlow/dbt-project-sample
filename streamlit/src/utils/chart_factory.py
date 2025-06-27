""""
Dynamic Chart Factory for Cortex Analyst

This module generates optimized visualizations based on chart intelligence recommendations.
Supports multiple visualization libraries and includes accessibility features.
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from typing import Dict, List, Optional, Any, Union
import warnings

try:
    from .chart_intelligence import ChartType, ChartRecommendation, DatasetAnalysis
except ImportError:
    from chart_intelligence import ChartType, ChartRecommendation, DatasetAnalysis

warnings.filterwarnings('ignore')

class DynamicChartFactory:
    """Generates optimized visualizations based on recommendations"""
    
    def __init__(self):
        self.default_colors = px.colors.qualitative.Set3
        self.continuous_colorscale = "viridis"
    
    def _set_dynamic_axis_range(self, fig: go.Figure, data: pd.DataFrame, x_col: str, y_col: str = None):
        """Set dynamic axis ranges based on actual data with appropriate padding"""
        
        # Set X-axis range
        if x_col in data.columns:
            x_min = data[x_col].min()
            x_max = data[x_col].max()
            
            # Add small padding for better visualization
            if pd.api.types.is_datetime64_any_dtype(data[x_col]):
                # For datetime, add 5% padding on each side
                time_range = x_max - x_min
                padding = time_range * 0.05
                x_min_padded = x_min - padding
                x_max_padded = x_max + padding
            elif pd.api.types.is_numeric_dtype(data[x_col]):
                # For numeric, add 5% padding on each side
                value_range = x_max - x_min
                padding = value_range * 0.05 if value_range > 0 else 1
                x_min_padded = x_min - padding
                x_max_padded = x_max + padding
            else:
                # For categorical, use the data as-is
                x_min_padded = x_min
                x_max_padded = x_max
            
            # Update x-axis range
            fig.update_xaxes(range=[x_min_padded, x_max_padded])
        
        # Set Y-axis range if specified
        if y_col and y_col in data.columns:
            y_min = data[y_col].min()
            y_max = data[y_col].max()
            
            if pd.api.types.is_numeric_dtype(data[y_col]):
                # For numeric Y-axis, add 10% padding for better visibility
                value_range = y_max - y_min
                padding = value_range * 0.1 if value_range > 0 else 1
                y_min_padded = y_min - padding
                y_max_padded = y_max + padding
                
                # Update y-axis range
                fig.update_yaxes(range=[y_min_padded, y_max_padded])
        
    def create_chart(self, 
                    recommendation: ChartRecommendation, 
                    data: pd.DataFrame,
                    analysis: DatasetAnalysis,
                    title: str = "",
                    height: int = 400) -> go.Figure:
        """
        Generate optimized chart based on recommendation
        
        Args:
            recommendation: Chart recommendation with metadata
            data: DataFrame to visualize
            analysis: Dataset analysis results
            title: Optional chart title
            height: Chart height in pixels
            
        Returns:
            Plotly figure object
        """
        
        if data.empty:
            return self._create_empty_chart(title or "No Data Available")
        
        chart_type = recommendation.chart_type
        
        # Route to appropriate chart creation method
        chart_methods = {
            ChartType.BAR: self._create_bar_chart,
            ChartType.HORIZONTAL_BAR: self._create_horizontal_bar_chart,
            ChartType.LINE: self._create_line_chart,
            ChartType.AREA: self._create_area_chart,
            ChartType.SCATTER: self._create_scatter_chart,
            ChartType.PIE: self._create_pie_chart,
            ChartType.HISTOGRAM: self._create_histogram,
            ChartType.BOX: self._create_box_plot,
            ChartType.HEATMAP: self._create_heatmap,
            ChartType.TABLE: self._create_table_view,
            ChartType.STACKED_BAR: self._create_stacked_bar_chart,
            ChartType.STACKED_AREA: self._create_stacked_area_chart,
            ChartType.TREEMAP: self._create_treemap_chart,
            ChartType.GROUPED_BAR: self._create_grouped_bar_chart
        }
        
        if chart_type in chart_methods:
            try:
                fig = chart_methods[chart_type](recommendation, data, analysis)
                
                # Apply common styling
                self._apply_common_styling(fig, title, height)
                
                # Add accessibility features
                self._add_accessibility_features(fig, recommendation)
                
                return fig
                
            except Exception as e:
                st.warning(f"Error creating {chart_type.value} chart: {str(e)}")
                return self._create_fallback_chart(data, title)
        else:
            return self._create_fallback_chart(data, title)
    
    def _create_bar_chart(self, 
                         recommendation: ChartRecommendation, 
                         data: pd.DataFrame,
                         analysis: DatasetAnalysis) -> go.Figure:
        """Create optimized bar chart"""
        
        x_col = recommendation.x_axis
        y_col = recommendation.y_axis
        
        # Validate that we have proper axes specified
        if not x_col or not y_col:
            # Try to infer appropriate columns
            categorical_cols = [col for col in data.columns if data[col].dtype in ['object', 'category']]
            numeric_cols = [col for col in data.columns if pd.api.types.is_numeric_dtype(data[col])]
            
            if categorical_cols and numeric_cols:
                x_col = categorical_cols[0]
                y_col = numeric_cols[0]
            elif len(data.columns) >= 2:
                # Use first two columns as fallback
                x_col = data.columns[0]
                y_col = data.columns[1]
            else:
                # Fall back to table view for problematic data
                return self._create_fallback_chart(data, "Bar chart requires categorical and numeric data")
        
        # Ensure columns exist in data
        if x_col not in data.columns or y_col not in data.columns:
            return self._create_fallback_chart(data, "Specified columns not found in data")
        
        # Handle aggregation if needed
        if recommendation.aggregation_method == "count" and len(data.columns) == 1:
            # Count frequency of categories
            value_counts = data[x_col].value_counts()
            plot_data = pd.DataFrame({
                x_col: value_counts.index,
                'count': value_counts.values
            })
            y_col = 'count'
        elif analysis.needs_aggregation and x_col in data.columns and y_col in data.columns:
            # Perform intelligent aggregation
            plot_data = self._apply_intelligent_aggregation(data, analysis, x_col, y_col)
        else:
            plot_data = data.copy()
        
        # Implement top-N filtering for high cardinality categorical data
        if x_col in plot_data.columns and plot_data[x_col].dtype in ['object', 'category']:
            unique_categories = plot_data[x_col].nunique()
            if unique_categories > 15:  # Reduced threshold per PRD
                # Keep top 10 categories by value
                if y_col in plot_data.columns and pd.api.types.is_numeric_dtype(plot_data[y_col]):
                    top_categories = plot_data.nlargest(10, y_col)
                    plot_data = top_categories
                    # Add note about filtering
                    if hasattr(recommendation, 'warnings'):
                        recommendation.warnings.append(f"Showing top 10 of {unique_categories} categories for clarity")
        
        # Clean data for safe chart rendering
        from .snowflake_compatibility import clean_chart_data
        plot_data = clean_chart_data(plot_data)
        
        # Ensure y_col is numeric for bar chart
        if not pd.api.types.is_numeric_dtype(plot_data[y_col]):
            try:
                plot_data[y_col] = pd.to_numeric(plot_data[y_col], errors='coerce')
                plot_data = clean_chart_data(plot_data)  # Clean again after conversion
            except:
                return self._create_fallback_chart(data, "Y-axis data cannot be converted to numeric")
        
        # Sort by value for better readability (limit to reasonable size)
        if y_col in plot_data.columns and len(plot_data) <= 1000:
            plot_data = plot_data.sort_values(y_col, ascending=True)
        
        try:
            fig = px.bar(
                plot_data, 
                x=x_col, 
                y=y_col,
                color_discrete_sequence=self.default_colors,
                text=y_col if len(plot_data) < 20 else None  # Show values for small datasets
            )
            
            # Improve text positioning
            if len(plot_data) < 20:
                fig.update_traces(texttemplate='%{text}', textposition='outside')
            
            # Enforce zero baseline for quantitative integrity
            if pd.api.types.is_numeric_dtype(plot_data[y_col]):
                y_min = plot_data[y_col].min()
                y_max = plot_data[y_col].max()
                if y_min >= 0:
                    # All positive values - start from zero
                    fig.update_yaxes(range=[0, y_max * 1.1])
                elif y_max <= 0:
                    # All negative values - end at zero
                    fig.update_yaxes(range=[y_min * 1.1, 0])
                # For mixed positive/negative, let Plotly handle the range naturally
            
            return fig
        
        except Exception as e:
            # If bar chart fails, fall back to table view
            return self._create_fallback_chart(data, f"Bar chart creation failed: {str(e)}")
    
    def _create_horizontal_bar_chart(self, 
                                   recommendation: ChartRecommendation, 
                                   data: pd.DataFrame,
                                   analysis: DatasetAnalysis) -> go.Figure:
        """Create horizontal bar chart for better label readability"""
        
        x_col = recommendation.x_axis  # This will be the value axis
        y_col = recommendation.y_axis  # This will be the category axis
        
        plot_data = data.copy()
        
        # Sort by value
        if x_col in plot_data.columns:
            plot_data = plot_data.sort_values(x_col, ascending=True)
        
        fig = px.bar(
            plot_data, 
            x=x_col, 
            y=y_col,
            orientation='h',
            color_discrete_sequence=self.default_colors,
            text=x_col if len(plot_data) < 15 else None
        )
        
        if len(plot_data) < 15:
            fig.update_traces(texttemplate='%{text}', textposition='outside')
        
        return fig
    
    def _create_line_chart(self, 
                          recommendation: ChartRecommendation, 
                          data: pd.DataFrame,
                          analysis: DatasetAnalysis) -> go.Figure:
        """Create line chart optimized for time series"""
        
        x_col = recommendation.x_axis
        y_col = recommendation.y_axis
        
        plot_data = data.copy()
        
        # Sort by x-axis for proper line connection
        if x_col in plot_data.columns:
            plot_data = plot_data.sort_values(x_col)
        
        fig = px.line(
            plot_data, 
            x=x_col, 
            y=y_col,
            markers=True if len(plot_data) < 50 else False,  # Show markers for small datasets
            line_shape='linear'
        )
        
        # Set dynamic axis ranges based on actual data
        self._set_dynamic_axis_range(fig, plot_data, x_col, y_col)
        
        # Add trend line if appropriate
        if len(plot_data) > 5 and y_col in analysis.numeric_columns:
            try:
                # Calculate manual trend line
                x_vals = plot_data[x_col].values if pd.api.types.is_numeric_dtype(plot_data[x_col]) else range(len(plot_data))
                y_vals = plot_data[y_col].values
                
                # Remove any NaN values
                if pd.api.types.is_numeric_dtype(plot_data[x_col]):
                    mask = ~(np.isnan(x_vals) | np.isnan(y_vals))
                    x_clean = x_vals[mask]
                    y_clean = y_vals[mask]
                else:
                    # For non-numeric x-axis, use index positions
                    mask = ~np.isnan(y_vals)
                    x_clean = np.arange(len(y_vals))[mask]
                    y_clean = y_vals[mask]
                
                if len(x_clean) > 3:
                    # Calculate linear trend
                    z = np.polyfit(x_clean, y_clean, 1)
                    p = np.poly1d(z)
                    
                    # Add trend line
                    fig.add_trace(go.Scatter(
                        x=plot_data[x_col] if pd.api.types.is_numeric_dtype(plot_data[x_col]) else plot_data.index,
                        y=p(x_clean),
                        mode='lines',
                        line=dict(dash='dash', color='rgba(255,0,0,0.5)', width=2),
                        name='Trend',
                        hovertemplate='Trend Line<extra></extra>'
                    ))
            except Exception:
                # If trend calculation fails, continue without trend line
                pass
        
        return fig
    
    def _create_area_chart(self, 
                          recommendation: ChartRecommendation, 
                          data: pd.DataFrame,
                          analysis: DatasetAnalysis) -> go.Figure:
        """Create area chart for magnitude emphasis"""
        
        x_col = recommendation.x_axis
        y_col = recommendation.y_axis
        
        plot_data = data.copy()
        
        if x_col in plot_data.columns:
            plot_data = plot_data.sort_values(x_col)
        
        fig = px.area(
            plot_data, 
            x=x_col, 
            y=y_col,
            line_shape='spline'
        )
        
        # Set dynamic axis ranges based on actual data
        self._set_dynamic_axis_range(fig, plot_data, x_col, y_col)
        
        return fig
    
    def _create_scatter_chart(self, 
                             recommendation: ChartRecommendation, 
                             data: pd.DataFrame,
                             analysis: DatasetAnalysis) -> go.Figure:
        """Create scatter plot with correlation analysis"""
        
        x_col = recommendation.x_axis
        y_col = recommendation.y_axis
        
        # Validate that we have proper axes specified
        if not x_col or not y_col:
            # Try to find two numeric columns
            numeric_cols = [col for col in data.columns if pd.api.types.is_numeric_dtype(data[col])]
            if len(numeric_cols) >= 2:
                x_col = numeric_cols[0]
                y_col = numeric_cols[1]
            elif len(data.columns) >= 2:
                # Use first two columns as fallback
                x_col = data.columns[0]
                y_col = data.columns[1]
            else:
                return self._create_fallback_chart(data, "Scatter plot requires two numeric columns")
        
        # Ensure columns exist in data
        if x_col not in data.columns or y_col not in data.columns:
            return self._create_fallback_chart(data, "Specified columns not found in data")
        
        plot_data = data.copy()
        
        # Ensure both columns are numeric
        for col in [x_col, y_col]:
            if not pd.api.types.is_numeric_dtype(plot_data[col]):
                try:
                    plot_data[col] = pd.to_numeric(plot_data[col], errors='coerce')
                except:
                    return self._create_fallback_chart(data, f"Column {col} cannot be converted to numeric")
        
        # Remove rows with NaN values in either column
        plot_data = plot_data.dropna(subset=[x_col, y_col])
        
        if len(plot_data) == 0:
            return self._create_fallback_chart(data, "No valid data points for scatter plot")
        
        # Sample data if too large for performance
        if len(plot_data) > 5000:
            plot_data = plot_data.sample(n=5000, random_state=42)
        
        # Check for discrete numeric variables that might need jitter
        x_unique_ratio = plot_data[x_col].nunique() / len(plot_data)
        y_unique_ratio = plot_data[y_col].nunique() / len(plot_data)
        needs_jitter = (x_unique_ratio < 0.1 or y_unique_ratio < 0.1) and len(plot_data) > 50
        
        try:
            if needs_jitter:
                # Add small random jitter to reduce overplotting
                jitter_amount = 0.1
                if x_unique_ratio < 0.1:
                    plot_data[x_col] = plot_data[x_col] + np.random.normal(0, jitter_amount, len(plot_data))
                if y_unique_ratio < 0.1:
                    plot_data[y_col] = plot_data[y_col] + np.random.normal(0, jitter_amount, len(plot_data))
            
            fig = px.scatter(
                plot_data, 
                x=x_col, 
                y=y_col,
                opacity=0.6 if needs_jitter else 0.7
            )
            
            # Set dynamic axis ranges based on actual data
            self._set_dynamic_axis_range(fig, plot_data, x_col, y_col)
            
            # Add manual trend line if enough data points
            if len(plot_data) > 10:
                try:
                    # Simple linear regression using numpy
                    x_vals = plot_data[x_col].values
                    y_vals = plot_data[y_col].values
                    
                    # Remove any NaN values
                    mask = ~(np.isnan(x_vals) | np.isnan(y_vals))
                    x_clean = x_vals[mask]
                    y_clean = y_vals[mask]
                    
                    if len(x_clean) > 5:  # Need at least 5 points for trend
                        # Calculate linear trend
                        z = np.polyfit(x_clean, y_clean, 1)
                        p = np.poly1d(z)
                        
                        # Add trend line
                        fig.add_trace(go.Scatter(
                            x=x_clean,
                            y=p(x_clean),
                            mode='lines',
                            name='Trend',
                            line=dict(dash='dash', color='red', width=2),
                            hovertemplate='Trend Line<extra></extra>'
                        ))
                except Exception:
                    # If trend calculation fails, continue without trend line
                    pass
            
            # Add correlation coefficient if numeric
            if len(plot_data) > 5:
                try:
                    corr = plot_data[x_col].corr(plot_data[y_col])
                    if not np.isnan(corr):
                        fig.add_annotation(
                            x=0.02, y=0.98,
                            xref="paper", yref="paper",
                            text=f"Correlation: {corr:.3f}",
                            showarrow=False,
                            bgcolor="rgba(255,255,255,0.8)",
                            bordercolor="gray",
                            borderwidth=1
                        )
                except Exception:
                    # If correlation calculation fails, continue without it
                    pass
            
            return fig
        
        except Exception as e:
            return self._create_fallback_chart(data, f"Scatter plot creation failed: {str(e)}")
    
    def _create_pie_chart(self, 
                         recommendation: ChartRecommendation, 
                         data: pd.DataFrame,
                         analysis: DatasetAnalysis) -> go.Figure:
        """Create pie chart for composition analysis"""
        
        color_col = recommendation.color_by
        
        if len(data.columns) == 1:
            # Single column - show value counts
            value_counts = data[color_col].value_counts()
            labels = value_counts.index
            values = value_counts.values
        else:
            # Use specified columns
            labels = data[color_col] if color_col else data.iloc[:, 0]
            values = data.iloc[:, 1] if len(data.columns) > 1 else [1] * len(data)
        
        fig = px.pie(
            values=values,
            names=labels,
            color_discrete_sequence=self.default_colors
        )
        
        # Improve label formatting
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Value: %{value}<br>Percentage: %{percent}<extra></extra>'
        )
        
        return fig
    
    def _create_histogram(self, 
                         recommendation: ChartRecommendation, 
                         data: pd.DataFrame,
                         analysis: DatasetAnalysis) -> go.Figure:
        """Create histogram for distribution analysis"""
        
        x_col = recommendation.x_axis
        
        plot_data = data[x_col].dropna()
        
        # Determine optimal number of bins
        n_bins = min(50, max(10, int(np.sqrt(len(plot_data)))))
        
        fig = px.histogram(
            x=plot_data,
            nbins=n_bins,
            marginal="box",  # Add box plot on top
            opacity=0.7
        )
        
        # Add statistical annotations
        mean_val = plot_data.mean()
        median_val = plot_data.median()
        
        fig.add_vline(
            x=mean_val, 
            line_dash="dash", 
            line_color="red",
            annotation_text=f"Mean: {mean_val:.2f}"
        )
        
        fig.add_vline(
            x=median_val, 
            line_dash="dot", 
            line_color="blue",
            annotation_text=f"Median: {median_val:.2f}"
        )
        
        return fig
    
    def _create_box_plot(self, 
                        recommendation: ChartRecommendation, 
                        data: pd.DataFrame,
                        analysis: DatasetAnalysis) -> go.Figure:
        """Create box plot for outlier analysis"""
        
        y_col = recommendation.y_axis
        
        # Check if we have pre-calculated quartile summary statistics
        quartile_columns = self._detect_quartile_summary_columns(data)
        
        if quartile_columns:
            # Create custom box plot from summary statistics
            fig = self._create_summary_box_plot(data, quartile_columns)
        else:
            # Standard box plot for raw data
            fig = px.box(
                data, 
                y=y_col,
                points="outliers"  # Show outlier points
            )
            
            # Add summary statistics
            col_analysis = analysis.columns[y_col]
            if col_analysis.mean is not None:
                fig.add_annotation(
                    x=0.02, y=0.98,
                    xref="paper", yref="paper",
                    text=f"Mean: {col_analysis.mean:.2f}<br>Std: {col_analysis.std:.2f}",
                    showarrow=False,
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="gray",
                    borderwidth=1,
                    align="left"
                )
        
        return fig
    
    def _detect_quartile_summary_columns(self, data: pd.DataFrame) -> dict:
        """Detect if data contains quartile summary statistics columns"""
        
        columns = [col.lower() for col in data.columns]
        quartile_mapping = {}
        
        # Common quartile column patterns
        patterns = {
            'min': ['min', 'minimum', 'min_'],
            'q1': ['q1', 'first_quartile', '1st_quartile', 'lower_quartile'],
            'median': ['median', 'q2', 'second_quartile', '2nd_quartile'],
            'q3': ['q3', 'third_quartile', '3rd_quartile', 'upper_quartile'],
            'max': ['max', 'maximum', 'max_']
        }
        
        # Find matching columns
        for stat, pattern_list in patterns.items():
            for col in data.columns:
                col_lower = col.lower()
                if any(pattern in col_lower for pattern in pattern_list):
                    quartile_mapping[stat] = col
                    break
        
        # Need at least median and two quartiles to create a meaningful box plot
        required_stats = ['median']
        optional_stats = ['min', 'q1', 'q3', 'max']
        
        if 'median' in quartile_mapping and len(quartile_mapping) >= 3:
            return quartile_mapping
        
        return {}
    
    def _create_summary_box_plot(self, data: pd.DataFrame, quartile_columns: dict) -> go.Figure:
        """Create box plot from pre-calculated quartile summary statistics"""
        
        # Extract quartile values (assuming single row of summary stats)
        row = data.iloc[0] if len(data) > 0 else data.iloc[0]
        
        # Get values, with fallbacks
        min_val = row[quartile_columns['min']] if 'min' in quartile_columns else None
        q1_val = row[quartile_columns['q1']] if 'q1' in quartile_columns else None
        median_val = row[quartile_columns['median']] if 'median' in quartile_columns else None
        q3_val = row[quartile_columns['q3']] if 'q3' in quartile_columns else None
        max_val = row[quartile_columns['max']] if 'max' in quartile_columns else None
        
        # Create custom box plot using go.Box with explicit quartile values
        fig = go.Figure()
        
        # If we have all quartile values, create a proper box plot
        if all(val is not None for val in [min_val, q1_val, median_val, q3_val, max_val]):
            fig.add_trace(go.Box(
                q1=[q1_val],
                median=[median_val],
                q3=[q3_val],
                lowerfence=[min_val],
                upperfence=[max_val],
                name="Sentiment Quartiles",
                boxpoints=False,
                fillcolor='rgba(135, 206, 235, 0.5)',
                line=dict(color='rgb(8, 81, 156)', width=2)
            ))
        else:
            # Fallback: create a simpler visualization with available values
            values = []
            labels = []
            
            for stat, col in quartile_columns.items():
                if col in data.columns:
                    values.append(row[col])
                    labels.append(stat.replace('_', ' ').title())
            
            # Create a scatter plot showing the quartile points
            fig.add_trace(go.Scatter(
                x=['Quartile Statistics'] * len(values),
                y=values,
                mode='markers+text',
                text=labels,
                textposition='middle right',
                marker=dict(
                    size=12,
                    color=['red', 'orange', 'green', 'orange', 'red'][:len(values)],
                    symbol='diamond'
                ),
                name="Quartile Values"
            ))
        
        # Add annotations with quartile information
        annotation_text = []
        for stat, col in quartile_columns.items():
            if col in data.columns:
                value = row[col]
                stat_name = stat.replace('_', ' ').title()
                if stat == 'q1':
                    stat_name = '1st Quartile'
                elif stat == 'q3':
                    stat_name = '3rd Quartile'
                annotation_text.append(f"{stat_name}: {value:.3f}")
        
        if annotation_text:
            fig.add_annotation(
                x=0.02, y=0.98,
                xref="paper", yref="paper",
                text="<br>".join(annotation_text),
                showarrow=False,
                bgcolor="rgba(255,255,255,0.9)",
                bordercolor="gray",
                borderwidth=1,
                align="left",
                font=dict(size=10)
            )
        
        # Update layout for better appearance
        fig.update_layout(
            title="Sentiment Score Quartile Analysis",
            yaxis_title="Sentiment Score",
            showlegend=False
        )
        
        return fig
    
    def _create_heatmap(self, 
                       recommendation: ChartRecommendation, 
                       data: pd.DataFrame,
                       analysis: DatasetAnalysis) -> go.Figure:
        """Create correlation heatmap"""
        
        # Use only numeric columns for correlation
        numeric_data = data.select_dtypes(include=[np.number])
        
        if numeric_data.empty or len(numeric_data.columns) < 2:
            return self._create_fallback_chart(data, "Heatmap requires numeric data")
        
        # Calculate correlation matrix
        corr_matrix = numeric_data.corr()
        
        fig = px.imshow(
            corr_matrix,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="RdBu_r",
            zmin=-1,
            zmax=1
        )
        
        return fig
    
    def _create_table_view(self, 
                          recommendation: ChartRecommendation, 
                          data: pd.DataFrame,
                          analysis: DatasetAnalysis) -> go.Figure:
        """Create interactive table view"""
        
        # For large datasets, show sample
        display_data = data.head(100) if len(data) > 100 else data
        
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=list(display_data.columns),
                fill_color='lightblue',
                align='left',
                font=dict(size=12, color='black')
            ),
            cells=dict(
                values=[display_data[col] for col in display_data.columns],
                fill_color='white',
                align='left',
                font=dict(size=11)
            )
        )])
        
        if len(data) > 100:
            fig.add_annotation(
                x=0.5, y=1.02,
                xref="paper", yref="paper",
                text=f"Showing first 100 rows of {len(data)} total rows",
                showarrow=False,
                font=dict(size=10, color="gray")
            )
        
        return fig
    
    def _apply_common_styling(self, fig: go.Figure, title: str, height: int):
        """Apply consistent styling across all charts"""
        
        fig.update_layout(
            title={
                'text': title,
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16, 'family': 'Arial, sans-serif'}
            },
            height=height,
            font=dict(family="Arial, sans-serif", size=12),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=40, r=40, t=60, b=40),
            showlegend=True if fig.data and len(fig.data) > 1 else False
        )
        
        # Update axes styling
        fig.update_xaxes(
            gridcolor='rgba(128,128,128,0.2)',
            linecolor='rgba(128,128,128,0.5)',
            tickfont=dict(size=10)
        )
        fig.update_yaxes(
            gridcolor='rgba(128,128,128,0.2)',
            linecolor='rgba(128,128,128,0.5)',
            tickfont=dict(size=10)
        )
    
    def _add_accessibility_features(self, fig: go.Figure, recommendation: ChartRecommendation):
        """Add accessibility features to charts"""
        
        # Add alt text
        if recommendation.accessibility_notes:
            fig.update_layout(
                annotations=[
                    dict(
                        text=recommendation.accessibility_notes,
                        x=0, y=0,
                        xref="paper", yref="paper",
                        showarrow=False,
                        visible=False  # Hidden but available to screen readers
                    )
                ]
            )
        
        # Ensure good color contrast
        if hasattr(fig.data[0], 'marker') and hasattr(fig.data[0].marker, 'color'):
            # Use accessible color palette
            fig.update_traces(
                marker=dict(
                    line=dict(width=1, color='rgba(0,0,0,0.3)')  # Add borders for better distinction
                )
            )
    
    def _create_empty_chart(self, title: str) -> go.Figure:
        """Create chart for empty datasets"""
        
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5,
            text="No data available to display",
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title=title,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            height=300
        )
        
        return fig
    
    def _create_fallback_chart(self, data: pd.DataFrame, title: str) -> go.Figure:
        """Create fallback chart when specific chart type fails"""
        
        if data.empty:
            return self._create_empty_chart(title)
        
        # Try to create a simple bar chart of first column
        try:
            first_col = data.columns[0]
            if data[first_col].dtype in ['object', 'category']:
                value_counts = data[first_col].value_counts().head(10)
                fig = px.bar(
                    x=value_counts.index,
                    y=value_counts.values,
                    title=f"Frequency of {first_col}"
                )
            else:
                fig = px.histogram(
                    data,
                    x=first_col,
                    title=f"Distribution of {first_col}"
                )
            
            return fig
            
        except Exception:
            return self._create_empty_chart("Unable to create visualization")
    
    def _apply_intelligent_aggregation(self, data: pd.DataFrame, analysis: DatasetAnalysis, x_col: str, y_col: str) -> pd.DataFrame:
        """Apply intelligent aggregation based on analysis suggestions"""
        if not analysis.needs_aggregation:
            return data
        
        try:
            # Get aggregation method for the y column
            agg_method = analysis.aggregation_suggestions.get(y_col, 'sum')
            
            # Group by categorical column and aggregate numeric column
            if x_col in data.columns and y_col in data.columns:
                if agg_method == 'mean':
                    aggregated = data.groupby(x_col)[y_col].mean().reset_index()
                elif agg_method == 'sum':
                    aggregated = data.groupby(x_col)[y_col].sum().reset_index()
                elif agg_method == 'max':
                    aggregated = data.groupby(x_col)[y_col].max().reset_index()
                elif agg_method == 'min':
                    aggregated = data.groupby(x_col)[y_col].min().reset_index()
                else:
                    aggregated = data.groupby(x_col)[y_col].sum().reset_index()  # Default to sum
                
                return aggregated
        except Exception:
            # If aggregation fails, return original data
            pass
        
        return data
    
    def _create_stacked_bar_chart(self, recommendation: ChartRecommendation, data: pd.DataFrame, analysis: DatasetAnalysis) -> go.Figure:
        """Create stacked bar chart for part-to-whole analysis with categories"""
        
        # For stacked bars, we need at least 3 columns: category, subcategory, value
        if len(data.columns) < 3:
            return self._create_fallback_chart(data, "Stacked bar chart requires at least 3 columns")
        
        # Try to identify the columns
        categorical_cols = [col for col in data.columns if data[col].dtype in ['object', 'category']]
        numeric_cols = [col for col in data.columns if pd.api.types.is_numeric_dtype(data[col])]
        
        if len(categorical_cols) < 2 or len(numeric_cols) < 1:
            return self._create_fallback_chart(data, "Stacked bar chart requires 2 categorical and 1 numeric column")
        
        x_col = categorical_cols[0]  # Main category
        color_col = categorical_cols[1]  # Subcategory for stacking
        y_col = numeric_cols[0]  # Value
        
        try:
            fig = px.bar(
                data,
                x=x_col,
                y=y_col,
                color=color_col,
                title="Stacked Bar Chart"
            )
            
            # Enforce zero baseline
            fig.update_yaxes(range=[0, None])
            
            return fig
        except Exception:
            return self._create_fallback_chart(data, "Error creating stacked bar chart")
    
    def _create_stacked_area_chart(self, recommendation: ChartRecommendation, data: pd.DataFrame, analysis: DatasetAnalysis) -> go.Figure:
        """Create stacked area chart for cumulative composition over time"""
        
        # Need temporal, categorical, and numeric columns
        temporal_cols = [col for col in data.columns if pd.api.types.is_datetime64_any_dtype(data[col])]
        categorical_cols = [col for col in data.columns if data[col].dtype in ['object', 'category']]
        numeric_cols = [col for col in data.columns if pd.api.types.is_numeric_dtype(data[col])]
        
        if not temporal_cols or not categorical_cols or not numeric_cols:
            return self._create_fallback_chart(data, "Stacked area chart requires temporal, categorical, and numeric data")
        
        x_col = temporal_cols[0]
        color_col = categorical_cols[0]
        y_col = numeric_cols[0]
        
        try:
            fig = px.area(
                data,
                x=x_col,
                y=y_col,
                color=color_col,
                title="Stacked Area Chart"
            )
            
            return fig
        except Exception:
            return self._create_fallback_chart(data, "Error creating stacked area chart")
    
    def _create_treemap_chart(self, recommendation: ChartRecommendation, data: pd.DataFrame, analysis: DatasetAnalysis) -> go.Figure:
        """Create treemap for hierarchical composition data"""
        
        # Need categorical and numeric columns
        categorical_cols = [col for col in data.columns if data[col].dtype in ['object', 'category']]
        numeric_cols = [col for col in data.columns if pd.api.types.is_numeric_dtype(data[col])]
        
        if not categorical_cols or not numeric_cols:
            return self._create_fallback_chart(data, "Treemap requires categorical and numeric data")
        
        # Use first categorical for labels and first numeric for values
        labels_col = categorical_cols[0]
        values_col = numeric_cols[0]
        
        try:
            # Aggregate data if needed
            if analysis.needs_aggregation:
                plot_data = data.groupby(labels_col)[values_col].sum().reset_index()
            else:
                plot_data = data.copy()
            
            fig = go.Figure(go.Treemap(
                labels=plot_data[labels_col],
                values=plot_data[values_col],
                parents=[""] * len(plot_data),  # Flat hierarchy
                textinfo="label+value+percent parent"
            ))
            
            fig.update_layout(title="Treemap Chart")
            
            return fig
        except Exception:
            return self._create_fallback_chart(data, "Error creating treemap chart")
    
    def _create_grouped_bar_chart(self, recommendation: ChartRecommendation, data: pd.DataFrame, analysis: DatasetAnalysis) -> go.Figure:
        """Create grouped bar chart for comparing multiple series"""
        
        # Need at least 3 columns: category, subcategory, value
        if len(data.columns) < 3:
            return self._create_fallback_chart(data, "Grouped bar chart requires at least 3 columns")
        
        categorical_cols = [col for col in data.columns if data[col].dtype in ['object', 'category']]
        numeric_cols = [col for col in data.columns if pd.api.types.is_numeric_dtype(data[col])]
        
        if len(categorical_cols) < 2 or len(numeric_cols) < 1:
            return self._create_fallback_chart(data, "Grouped bar chart requires 2 categorical and 1 numeric column")
        
        x_col = categorical_cols[0]
        color_col = categorical_cols[1]
        y_col = numeric_cols[0]
        
        try:
            fig = px.bar(
                data,
                x=x_col,
                y=y_col,
                color=color_col,
                barmode='group',
                title="Grouped Bar Chart"
            )
            
            # Enforce zero baseline
            fig.update_yaxes(range=[0, None])
            
            return fig
        except Exception:
            return self._create_fallback_chart(data, "Error creating grouped bar chart")

class ChartSelector:
    """Interactive chart selection component"""
    
    def __init__(self):
        self.factory = DynamicChartFactory()
    
    def render_chart_selector(self, 
                            recommendations: List[ChartRecommendation],
                            data: pd.DataFrame,
                            analysis: DatasetAnalysis,
                            key_prefix: str = "chart",
                            query_context: str = "") -> go.Figure:
        """
        Render interactive chart selector with recommendations
        
        Args:
            recommendations: List of chart recommendations
            data: DataFrame to visualize
            analysis: Dataset analysis results
            key_prefix: Unique key prefix for Streamlit components
            
        Returns:
            Selected chart figure
        """
        
        if not recommendations:
            return self.factory._create_empty_chart("No recommendations available")
        
        # Display primary recommendation with enhanced information
        primary_rec = recommendations[0]
        
        # Create confidence indicator
        confidence_color = "#22c55e" if primary_rec.confidence_score > 0.8 else "#f59e0b" if primary_rec.confidence_score > 0.6 else "#ef4444"
        
        st.markdown(f"""
            <div class="card-outline" style="margin-bottom: 16px; border-left: 4px solid {confidence_color};">
                <h4 style="color: {confidence_color}; margin-bottom: 8px;">
                    🎯 Recommended: {primary_rec.chart_type.value.replace('_', ' ').title()}
                </h4>
                <p style="margin: 0 0 8px 0; color: #666; font-size: 14px;">
                    <strong>Confidence:</strong> {primary_rec.confidence_score:.0%} • 
                    <strong>Effectiveness:</strong> {getattr(primary_rec, 'perceptual_effectiveness_score', 0.8):.0%} •
                    <strong>Intent Match:</strong> {getattr(primary_rec, 'user_intent_match', 0.7):.0%}
                </p>
                <p style="margin: 0; color: #444; font-size: 13px; line-height: 1.4;">
                    {primary_rec.reasoning}
                </p>
            </div>
        """, unsafe_allow_html=True)
        
        # Show detailed explanation if available
        if hasattr(primary_rec, 'detailed_explanation') and primary_rec.detailed_explanation:
            with st.expander("💡 Why this chart?", expanded=False):
                st.write(primary_rec.detailed_explanation)
        
        # Show warnings if any
        if hasattr(primary_rec, 'warnings') and primary_rec.warnings:
            with st.expander("⚠️ Important considerations", expanded=False):
                for warning in primary_rec.warnings:
                    st.warning(warning)
        
        # Chart type selector
        chart_options = [rec.chart_type.value.replace('_', ' ').title() for rec in recommendations]
        chart_reasoning = [rec.reasoning for rec in recommendations]
        
        selected_chart = st.selectbox(
            "Chart Type:",
            options=chart_options,
            index=0,
            key=f"{key_prefix}_type_selector",
            help="Select from recommended chart types based on your data characteristics"
        )
        

        
        # Find selected recommendation
        selected_index = chart_options.index(selected_chart)
        selected_recommendation = recommendations[selected_index]
        

        
        # Generate and display chart
        chart_title = f"{selected_chart} - {analysis.row_count:,} rows × {analysis.column_count} columns"
        
        try:
            fig = self.factory.create_chart(
                recommendation=selected_recommendation,
                data=data,
                analysis=analysis,
                title=chart_title,
                height=500
            )
            
            return fig
            
        except Exception as e:
            st.error(f"Error creating chart: {str(e)}")
            return self.factory._create_fallback_chart(data, "Chart Generation Error") 