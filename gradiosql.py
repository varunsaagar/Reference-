import gradio as gr
import time
import sqlite3
from google.cloud import bigquery
from google.api_core import exceptions
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Part, Tool

# [Previous DatabaseAnalyzer class code remains the same]

def process_query_with_details(project_id, dataset_id, query_text):
    """Wrapper function to process queries and capture processing details"""
    processing_details = []
    
    try:
        # Initialize analyzer with new project/dataset
        global BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
        BIGQUERY_PROJECT_ID = project_id
        BIGQUERY_DATASET_ID = dataset_id
        analyzer = DatabaseAnalyzer(use_bigquery=True)
        processing_details.append(f"✅ Initialized connection to project: {project_id}, dataset: {dataset_id}")
        
        # Process the query
        processing_details.append(f"🔄 Processing query: {query_text}")
        response = analyzer.process_query(query_text)
        processing_details.append("✅ Query processing complete")
        
        return response, "\n".join(processing_details)
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        processing_details.append(error_msg)
        return f"Error occurred: {str(e)}", "\n".join(processing_details)

# Sample projects and datasets
AVAILABLE_PROJECTS = [
    "vz-it-np-ienv-test-vegsdo-0",
    "vz-it-np-ienv-prod-vegsdo-0"
]

AVAILABLE_DATASETS = {
    "vz-it-np-ienv-test-vegsdo-0": ["vegas_monitoring", "vegas_analytics"],
    "vz-it-np-ienv-prod-vegsdo-0": ["prod_monitoring", "prod_analytics"]
}

def update_datasets(project):
    """Update dataset choices based on selected project"""
    return gr.Dropdown(choices=AVAILABLE_DATASETS.get(project, []))

def clear_outputs():
    """Clear all output fields"""
    return "", "", ""

with gr.Blocks(title="NL to SQL - Verizon") as demo:
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
        outputs=[output, processing_details]
    )
    
    clear_btn.click(
        fn=clear_outputs,
        inputs=[],
        outputs=[query_text, output, processing_details]
    )

if __name__ == "__main__":
    # Launch with specific host and port
    demo.launch(
        server_name="0.0.0.0",  # Listen on all network interfaces
        server_port=7860,
        share=False  # Set to True if you want to create a public URL
    )
