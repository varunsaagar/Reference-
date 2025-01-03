import gradio as gr
import time
import sqlite3
from google.cloud import bigquery
from google.api_core import exceptions
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Part, Tool

# Your existing DatabaseAnalyzer class code here (unchanged)
# ... (keep all the existing code)

def process_query_with_details(project_id, dataset_id, query_text):
    """Wrapper function to process queries and capture processing details"""
    global current_analyzer
    processing_details = []
    
    try:
        # Initialize analyzer if needed or if project/dataset changed
        if not hasattr(process_query_with_details, 'current_config') or \
           process_query_with_details.current_config != (project_id, dataset_id):
            global BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
            BIGQUERY_PROJECT_ID = project_id
            BIGQUERY_DATASET_ID = dataset_id
            current_analyzer = DatabaseAnalyzer(use_bigquery=True)
            process_query_with_details.current_config = (project_id, dataset_id)
            processing_details.append(f"✅ Initialized connection to project: {project_id}, dataset: {dataset_id}")
        
        # Process the query
        processing_details.append(f"🔄 Processing query: {query_text}")
        response = current_analyzer.process_query(query_text)
        processing_details.append("✅ Query processing complete")
        
        return {
            "result": response,
            "details": "\n".join(processing_details)
        }
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        processing_details.append(error_msg)
        return {
            "result": f"Error occurred: {str(e)}",
            "details": "\n".join(processing_details)
        }

# Sample projects and datasets (replace with your actual options)
AVAILABLE_PROJECTS = [
    "vz-it-np-ienv-test-vegsdo-0",
    "vz-it-np-ienv-prod-vegsdo-0"
]

AVAILABLE_DATASETS = {
    "vz-it-np-ienv-test-vegsdo-0": ["vegas_monitoring", "vegas_analytics"],
    "vz-it-np-ienv-prod-vegsdo-0": ["prod_monitoring", "prod_analytics"]
}

def create_gradio_interface():
    """Create the Gradio interface"""
    
    def update_datasets(project):
        """Update dataset choices based on selected project"""
        return gr.Dropdown(choices=AVAILABLE_DATASETS.get(project, []))
    
    with gr.Blocks(title="NL to SQL - Verizon", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# 🔍 NL to SQL - Verizon")
        gr.Markdown("Convert natural language questions into SQL queries and get results from BigQuery")
        
        with gr.Row():
            project = gr.Dropdown(
                choices=AVAILABLE_PROJECTS,
                label="Select Project",
                value=AVAILABLE_PROJECTS[0]
            )
            dataset = gr.Dropdown(
                choices=AVAILABLE_DATASETS[AVAILABLE_PROJECTS[0]],
                label="Select Dataset",
                value=AVAILABLE_DATASETS[AVAILABLE_PROJECTS[0]][0]
            )
        
        query_text = gr.Textbox(
            label="Enter your question",
            placeholder="Example: What are the most recent API status records?",
            lines=3
        )
        
        with gr.Row():
            submit_btn = gr.Button("Submit", variant="primary")
            clear_btn = gr.Button("Clear")
        
        with gr.Accordion("Processing Details", open=False) as details_accordion:
            processing_details = gr.Textbox(
                label="Processing Steps",
                lines=10,
                interactive=False
            )
        
        output = gr.Textbox(
            label="Result",
            lines=10,
            interactive=False
        )
        
        # Event handlers
        project.change(
            fn=update_datasets,
            inputs=[project],
            outputs=[dataset]
        )
        
        submit_btn.click(
            fn=process_query_with_details,
            inputs=[project, dataset, query_text],
            outputs=[
                gr.JSON(value={"result": "", "details": ""}, visible=False),
                output,
                processing_details
            ],
            api_name="process_query"
        )
        
        clear_btn.click(
            fn=lambda: ("", "", ""),
            inputs=[],
            outputs=[query_text, output, processing_details]
        )
        
    return demo

# Launch the application
if __name__ == "__main__":
    demo = create_gradio_interface()
    demo.launch(
        share=True,
        server_name="0.0.0.0",
        server_port=7860,
        auth=("admin", "admin123")  # Remove or modify for production
    )


(text2sql) [domino@run-677775f203ca6841bc367eca-4kwk5 t2s]$ python3 gradiosql.py 
Traceback (most recent call last):
  File "/mnt/text2sql/lib64/python3.11/site-packages/httpx/_urlparse.py", line 409, in normalize_port
    port_as_int = int(port)
                  ^^^^^^^^^
ValueError: invalid literal for int() with base 10: ':1]'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/mnt/t2s/gradiosql.py", line 1, in <module>
    import gradio as gr
  File "/mnt/text2sql/lib64/python3.11/site-packages/gradio/__init__.py", line 3, in <module>
    import gradio._simple_templates
  File "/mnt/text2sql/lib64/python3.11/site-packages/gradio/_simple_templates/__init__.py", line 1, in <module>
    from .simpledropdown import SimpleDropdown
  File "/mnt/text2sql/lib64/python3.11/site-packages/gradio/_simple_templates/simpledropdown.py", line 7, in <module>
    from gradio.components.base import Component, FormComponent
  File "/mnt/text2sql/lib64/python3.11/site-packages/gradio/components/__init__.py", line 1, in <module>
    from gradio.components.annotated_image import AnnotatedImage
  File "/mnt/text2sql/lib64/python3.11/site-packages/gradio/components/annotated_image.py", line 14, in <module>
    from gradio import processing_utils, utils
  File "/mnt/text2sql/lib64/python3.11/site-packages/gradio/processing_utils.py", line 120, in <module>
    sync_client = httpx.Client(transport=sync_transport)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/mnt/text2sql/lib64/python3.11/site-packages/httpx/_client.py", line 697, in __init__
    self._mounts: dict[URLPattern, BaseTransport | None] = {
                                                           ^
  File "/mnt/text2sql/lib64/python3.11/site-packages/httpx/_client.py", line 698, in <dictcomp>
    URLPattern(key): None
    ^^^^^^^^^^^^^^^
  File "/mnt/text2sql/lib64/python3.11/site-packages/httpx/_utils.py", line 172, in __init__
    url = URL(pattern)
          ^^^^^^^^^^^^
  File "/mnt/text2sql/lib64/python3.11/site-packages/httpx/_urls.py", line 117, in __init__
    self._uri_reference = urlparse(url, **kwargs)
                          ^^^^^^^^^^^^^^^^^^^^^^^
  File "/mnt/text2sql/lib64/python3.11/site-packages/httpx/_urlparse.py", line 321, in urlparse
    parsed_port: int | None = normalize_port(port, scheme)
                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/mnt/text2sql/lib64/python3.11/site-packages/httpx/_urlparse.py", line 411, in normalize_port
    raise InvalidURL(f"Invalid port: {port!r}")
httpx.InvalidURL: Invalid port: ':1]'
