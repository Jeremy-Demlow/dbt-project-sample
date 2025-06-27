"""
Accessibility utilities for the Streamlit Customer Intelligence Hub.

This module provides utilities for enhancing accessibility including:
- Screen reader announcements
- Keyboard event handling
- Focus management
- ARIA live region management
- High contrast mode support
"""

import streamlit as st
from typing import Dict, List, Optional, Union
import time


def create_aria_live_region(message: str, level: str = "polite", region_id: str = None) -> str:
    """
    Create an ARIA live region for screen reader announcements.
    
    Args:
        message: The message to announce
        level: 'polite' or 'assertive' 
        region_id: Optional unique ID for the region
        
    Returns:
        HTML string for the live region
    """
    if region_id is None:
        region_id = f"aria-live-{int(time.time() * 1000)}"
    
    return f'''
    <div id="{region_id}" 
         aria-live="{level}" 
         aria-atomic="true" 
         class="sr-only">
        {message}
    </div>
    '''


def announce_to_screen_reader(message: str, level: str = "polite") -> None:
    """
    Announce a message to screen readers using ARIA live regions.
    
    Args:
        message: The message to announce
        level: 'polite' or 'assertive' for urgency
    """
    live_region_html = create_aria_live_region(message, level)
    st.markdown(live_region_html, unsafe_allow_html=True)


def create_skip_link(target_id: str, label: str) -> str:
    """
    Create a skip navigation link for keyboard users.
    
    Args:
        target_id: The ID of the target element to skip to
        label: The visible text for the skip link
        
    Returns:
        HTML string for the skip link
    """
    return f'''
    <a href="#{target_id}" 
       class="skip-link"
       onclick="document.getElementById('{target_id}').focus()">
        {label}
    </a>
    '''


def create_accessible_button(
    label: str,
    button_id: str,
    aria_label: str = None,
    aria_describedby: str = None,
    role: str = "button",
    onclick_js: str = None
) -> str:
    """
    Create an accessible button with proper ARIA attributes.
    
    Args:
        label: Button text
        button_id: Unique ID for the button
        aria_label: Accessible label if different from visible text
        aria_describedby: ID of element that describes the button
        role: ARIA role
        onclick_js: JavaScript to execute on click
        
    Returns:
        HTML string for the accessible button
    """
    aria_attrs = []
    if aria_label:
        aria_attrs.append(f'aria-label="{aria_label}"')
    if aria_describedby:
        aria_attrs.append(f'aria-describedby="{aria_describedby}"')
    
    onclick_attr = f'onclick="{onclick_js}"' if onclick_js else ''
    
    return f'''
    <button id="{button_id}" 
            role="{role}"
            {' '.join(aria_attrs)}
            {onclick_attr}
            class="accessible-button"
            tabindex="0"
            onkeydown="if(event.key==='Enter'||event.key===' '){{event.preventDefault();this.click();}}">
        {label}
    </button>
    '''


def create_accessible_form_field(
    field_type: str,
    field_id: str,
    label: str,
    placeholder: str = None,
    aria_describedby: str = None,
    required: bool = False,
    invalid: bool = False,
    error_message_id: str = None
) -> str:
    """
    Create an accessible form field with proper labeling.
    
    Args:
        field_type: Type of input field
        field_id: Unique ID for the field
        label: Label text
        placeholder: Placeholder text
        aria_describedby: ID of describing element
        required: Whether field is required
        invalid: Whether field has validation errors
        error_message_id: ID of error message element
        
    Returns:
        HTML string for the accessible form field
    """
    aria_attrs = []
    if aria_describedby:
        aria_attrs.append(f'aria-describedby="{aria_describedby}"')
    if required:
        aria_attrs.append('aria-required="true"')
    if invalid:
        aria_attrs.append('aria-invalid="true"')
        if error_message_id:
            aria_attrs.append(f'aria-errormessage="{error_message_id}"')
    
    placeholder_attr = f'placeholder="{placeholder}"' if placeholder else ''
    
    return f'''
    <div class="form-field-container">
        <label for="{field_id}" class="form-label">
            {label}
            {"<span aria-hidden='true'>*</span>" if required else ""}
        </label>
        <{field_type} id="{field_id}"
                      {' '.join(aria_attrs)}
                      {placeholder_attr}
                      class="form-field"
                      />
    </div>
    '''


def create_expandable_section(
    trigger_id: str,
    content_id: str,
    trigger_text: str,
    content_html: str,
    expanded: bool = False
) -> str:
    """
    Create an accessible expandable section with proper ARIA attributes.
    
    Args:
        trigger_id: ID for the trigger button
        content_id: ID for the content container
        trigger_text: Text for the trigger button
        content_html: HTML content to show/hide
        expanded: Initial state
        
    Returns:
        HTML string for the expandable section
    """
    expanded_state = "true" if expanded else "false"
    content_style = "display: block;" if expanded else "display: none;"
    
    return f'''
    <div class="expandable-section">
        <button id="{trigger_id}"
                aria-expanded="{expanded_state}"
                aria-controls="{content_id}"
                class="expandable-trigger"
                onclick="toggleExpandableSection('{trigger_id}', '{content_id}')"
                onkeydown="if(event.key==='Enter'||event.key===' '){{event.preventDefault();this.click();}}">
            <span aria-hidden="true">{'▼' if expanded else '▶'}</span>
            {trigger_text}
        </button>
        <div id="{content_id}" 
             aria-labelledby="{trigger_id}"
             class="expandable-content"
             style="{content_style}">
            {content_html}
        </div>
    </div>
    '''


def get_javascript_utilities() -> str:
    """
    Return JavaScript utilities for accessibility features.
    
    Returns:
        JavaScript code for accessibility functions
    """
    return '''
    <script>
    // Toggle expandable section
    function toggleExpandableSection(triggerId, contentId) {
        const trigger = document.getElementById(triggerId);
        const content = document.getElementById(contentId);
        const isExpanded = trigger.getAttribute('aria-expanded') === 'true';
        
        // Update ARIA state
        trigger.setAttribute('aria-expanded', !isExpanded);
        
        // Update visual state
        if (isExpanded) {
            content.style.display = 'none';
            trigger.querySelector('span[aria-hidden]').textContent = '▶';
        } else {
            content.style.display = 'block';
            trigger.querySelector('span[aria-hidden]').textContent = '▼';
        }
        
        // Announce state change
        const announcement = isExpanded ? 'Section collapsed' : 'Section expanded';
        announceToScreenReader(announcement, 'polite');
    }
    
    // Announce message to screen reader
    function announceToScreenReader(message, level = 'polite') {
        const announcement = document.createElement('div');
        announcement.setAttribute('aria-live', level);
        announcement.setAttribute('aria-atomic', 'true');
        announcement.className = 'sr-only';
        announcement.textContent = message;
        
        document.body.appendChild(announcement);
        
        // Remove after announcement
        setTimeout(() => {
            document.body.removeChild(announcement);
        }, 1000);
    }
    
    // Focus management
    function manageFocus(elementId) {
        const element = document.getElementById(elementId);
        if (element) {
            element.focus();
            element.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
    
    // Enhanced keyboard navigation
    function enhanceKeyboardNavigation() {
        document.addEventListener('keydown', function(event) {
            // Escape key handling
            if (event.key === 'Escape') {
                // Close any open expandable sections
                const expandedSections = document.querySelectorAll('[aria-expanded="true"]');
                expandedSections.forEach(section => {
                    section.click();
                });
                
                // Clear form inputs if focused
                if (document.activeElement.tagName === 'INPUT' || 
                    document.activeElement.tagName === 'TEXTAREA') {
                    if (event.ctrlKey) {
                        document.activeElement.value = '';
                        announceToScreenReader('Input cleared', 'polite');
                    }
                }
            }
            
            // Ctrl+Enter for quick submit
            if (event.key === 'Enter' && event.ctrlKey) {
                const submitButton = document.querySelector('button[type="submit"], .primary-action-button');
                if (submitButton) {
                    submitButton.click();
                }
            }
        });
    }
    
    // Initialize accessibility features
    document.addEventListener('DOMContentLoaded', function() {
        enhanceKeyboardNavigation();
    });
    </script>
    '''


def create_loading_announcement(operation: str, estimated_time: str = None) -> str:
    """
    Create an accessible loading announcement.
    
    Args:
        operation: Description of the operation being performed
        estimated_time: Optional estimated completion time
        
    Returns:
        HTML string for loading announcement
    """
    time_info = f" Estimated time: {estimated_time}." if estimated_time else ""
    message = f"{operation} in progress.{time_info} Please wait."
    
    return f'''
    <div aria-live="assertive" 
         aria-atomic="true" 
         class="loading-announcement sr-only">
        {message}
    </div>
    '''


def create_error_announcement(error_message: str, suggestion: str = None) -> str:
    """
    Create an accessible error announcement with recovery suggestions.
    
    Args:
        error_message: The error message
        suggestion: Optional suggestion for recovery
        
    Returns:
        HTML string for error announcement
    """
    full_message = error_message
    if suggestion:
        full_message += f" {suggestion}"
    
    return f'''
    <div role="alert" 
         aria-live="assertive" 
         class="error-announcement">
        <span class="sr-only">Error: </span>
        {full_message}
    </div>
    '''


def create_success_announcement(message: str) -> str:
    """
    Create an accessible success announcement.
    
    Args:
        message: The success message
        
    Returns:
        HTML string for success announcement
    """
    return f'''
    <div role="status" 
         aria-live="polite" 
         class="success-announcement">
        <span class="sr-only">Success: </span>
        {message}
    </div>
    '''


# Keyboard navigation utilities
KEYBOARD_SHORTCUTS = {
    'submit': 'Ctrl+Enter',
    'clear': 'Escape',
    'next_section': 'Tab',
    'previous_section': 'Shift+Tab',
    'expand_collapse': 'Enter or Space',
    'close_modal': 'Escape'
}


def get_keyboard_help_text() -> str:
    """
    Generate help text for keyboard shortcuts.
    
    Returns:
        Formatted help text for keyboard navigation
    """
    help_items = []
    for action, shortcut in KEYBOARD_SHORTCUTS.items():
        help_items.append(f"• {action.replace('_', ' ').title()}: {shortcut}")
    
    return "Keyboard shortcuts:\n" + "\n".join(help_items)


def create_keyboard_help_section() -> str:
    """
    Create an accessible keyboard help section.
    
    Returns:
        HTML string for keyboard help section
    """
    help_text = get_keyboard_help_text()
    
    return f'''
    <details class="keyboard-help">
        <summary>Keyboard Navigation Help</summary>
        <div class="keyboard-help-content">
            <pre aria-label="Keyboard shortcuts list">{help_text}</pre>
        </div>
    </details>
    ''' 