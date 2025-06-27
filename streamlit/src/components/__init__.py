"""
Components package for the Customer Analytics Dashboard.
"""

from typing import Dict, Callable, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Component:
    """Represents a dashboard component with its metadata and render function."""
    name: str
    render_func: Callable
    display_name: str
    icon: str
    order: int

class ComponentRegistry:
    """Registry for managing dashboard components."""
    
    def __init__(self):
        self._components: Dict[str, Component] = {}
    
    def register(self, component: Component) -> None:
        """Register a new component."""
        self._components[component.name] = component
    
    def get_component(self, name: str) -> Component:
        """Get a component by name."""
        return self._components[name]
    
    def get_all_components(self) -> list[Component]:
        """Get all components sorted by their order."""
        return sorted(self._components.values(), key=lambda x: x.order)
    
    def render_component(self, name: str, *args, **kwargs) -> Any:
        """Render a component by name."""
        return self._components[name].render_func(*args, **kwargs)

# Create the global registry instance
registry = ComponentRegistry()

# Import all component render functions
from .overview import render_overview
from .sentiment_experience import render_sentiment_experience
from .support_ops import render_support_ops_dashboard
from .product_feedback import render_product_feedback
from .segmentation import render_segmentation
from .cortex_analyst import render_cortex_analyst_tab

# Register all components
registry.register(Component(
    name="overview",
    render_func=render_overview,
    display_name="Overview",
    icon="📊",
    order=1
))

registry.register(Component(
    name="sentiment_experience",
    render_func=render_sentiment_experience,
    display_name="Sentiment & Experience",
    icon="😊",
    order=2
))

registry.register(Component(
    name="support_ops",
    render_func=render_support_ops_dashboard,
    display_name="Support Operations",
    icon="💬",
    order=3
))

registry.register(Component(
    name="product_feedback",
    render_func=render_product_feedback,
    display_name="Product Feedback",
    icon="💡",
    order=4
))

registry.register(Component(
    name="segmentation",
    render_func=render_segmentation,
    display_name="Segmentation",
    icon="🎯",
    order=5
))

registry.register(Component(
    name="cortex_analyst",
    render_func=render_cortex_analyst_tab,
    display_name="Cortex Analyst",
    icon="🧠",
    order=6
))

__all__ = ['registry'] 