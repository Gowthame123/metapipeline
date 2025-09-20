
import streamlit as st
import search
import add_edit
import ai_assistant
from collections import defaultdict
import uuid
import database

# Set page configuration
st.set_page_config(layout="wide", page_title="Metadata Pipeline Manager")

# Initialize the database
database.init_db()

# Initialize session state for pipelines
if 'pipelines' not in st.session_state:
    st.session_state['pipelines'] = database.get_all_pipelines()

# Initialize other session state variables
if 'current_view' not in st.session_state:
    st.session_state['current_view'] = 'search'
if 'edit_pipeline_id' not in st.session_state:
    st.session_state['edit_pipeline_id'] = None
if 'current_pipeline_layer' not in st.session_state:
    st.session_state['current_pipeline_layer'] = "L0"
if 'owner' not in st.session_state:
    st.session_state['owner'] = ""    
if 'form_visible' not in st.session_state:
    st.session_state['form_visible'] = False
if 'ai_collected_data' not in st.session_state:
    st.session_state['ai_collected_data'] = None

# Header and Navigation
st.title("Metadata Pipeline Manager")
col1, col2, col3 = st.columns([4, 0.5, 0.5])

with col1:
    st.write("Create, search, and manage metadata for your Databricks data pipelines")
with col2:
    if st.button("➕ New AI Pipeline ", use_container_width=True, type="primary"):
        st.session_state.current_view = 'ai_assistant'
        st.session_state.form_visible = True  
        st.rerun() 
with col3:
    if st.button("➕ New Pipeline", use_container_width=True, type="primary"):
        st.session_state.current_view = 'add_edit'
        st.session_state.form_visible = True
        st.session_state.edit_pipeline_id = None
        st.session_state.current_pipeline_layer = "L0"
        st.rerun()

col1, col2, col3 = st.columns([1, 1, 1])

with col1:
    if st.button("🔍 Search Pipelines", use_container_width=True):
        st.session_state.current_view = 'search'
with col2:
    if st.button("📝 Add/Edit Pipeline", use_container_width=True):
        st.session_state.current_view = 'add_edit'
        st.session_state.form_visible = False
        st.session_state.edit_pipeline_id = None
        st.session_state.current_pipeline_layer = "L0"
with col3:
    if st.button("🤖 AI Assistant", use_container_width=True):
        st.session_state.current_view = 'ai_assistant'


st.markdown("---")

# Main content based on current view
if st.session_state.current_view == 'search':
    search.show()
elif st.session_state.current_view == 'add_edit':
    # --- MODIFICATION START ---
    prefill_data = st.session_state.pop('ai_collected_data', None)
    if prefill_data:
        add_edit.show(prefill_data=prefill_data)
    else:
        add_edit.show()
    # --- MODIFICATION END ---
elif st.session_state.current_view == 'ai_assistant':
    ai_assistant.show()