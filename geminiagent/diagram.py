import graphviz

def create_nl_to_sql_workflow_diagram():
    """
    Generates a detailed digraph (using graphviz) representing the NL-to-SQL system workflow,
    with a color scheme and style aligned with Verizon's brand.
    """

    # Verizon brand colors (adjust as needed based on official guidelines)
    verizon_red = "#e60000"  # Main Verizon Red
    verizon_dark_red = "#cc0000" # Darker shade for accents/emphasis
    verizon_black = "#000000"  # Black
    verizon_gray = "#f5f5f5"  # Light Gray (for backgrounds)
    verizon_dark_gray = "#666666" # Dark Gray (for text/outlines)
    verizon_white = "#ffffff"  # White
    

    dot = graphviz.Digraph(
        comment="NL-to-SQL System Workflow for Verizon",
        engine="dot",  # Use the 'dot' engine for a hierarchical layout
        graph_attr={
            "rankdir": "TB",  # Top-to-bottom flow
            "fontsize": "14",
            "fontname": "Arial",  # Verizon commonly uses Arial
            "nodesep": "0.6",  # Increased node separation
            "ranksep": "0.9",  # Increased rank separation
            "bgcolor": verizon_white, # Background color
        },
        node_attr={"shape": "box", "style": "rounded,filled", "fontsize": "12",
                   "fontname": "Arial", "color": verizon_dark_gray, "penwidth": "1.2"}, # Default node style
        edge_attr={"fontsize": "10", "fontname": "Arial", "color": verizon_black, "penwidth": "1.0"}, # Default edge style
    )

    # --- Nodes ---

    # User Interaction
    dot.node("User", "User", shape="ellipse", fillcolor=verizon_gray, fontcolor=verizon_black)
    dot.node("Query", "Enter Natural Language Query", shape="rectangle", fillcolor=verizon_white, fontcolor=verizon_black, color=verizon_red)

    # Gemini Agent (Orchestrator)
    dot.node("Agent", "Gemini Agent", shape="diamond", fillcolor=verizon_white, fontcolor=verizon_dark_red, color=verizon_dark_red)

    # NLU Components
    dot.node("Intent", "Extract Intent", fillcolor=verizon_white, fontcolor=verizon_black)
    dot.node("Entities", "Extract Entities", fillcolor=verizon_white, fontcolor=verizon_black)
    dot.node("Mapping", "Map Entities to Columns", fillcolor=verizon_white, fontcolor=verizon_black)

    # Prompt Engineering
    dot.node("Prompt", "Generate Prompt", fillcolor=verizon_white, fontcolor=verizon_black, color=verizon_red)

    # Gemini Model
    dot.node("Gemini", "Gemini 1.5 Pro", shape="cylinder", fillcolor=verizon_gray, fontcolor=verizon_black)

    # Function Declarations
    dot.node("GetSchema", "get_table_schema()", shape="rectangle", fillcolor=verizon_white, fontcolor=verizon_black)
    dot.node("ExecuteQuery", "execute_sql_query()", shape="rectangle", fillcolor=verizon_white, fontcolor=verizon_black)
    dot.node("GetValues", "get_distinct_column_values()", shape="rectangle", fillcolor=verizon_white, fontcolor=verizon_black)

    # BigQuery Database
    dot.node("BigQuery", "BigQuery Database", shape="cylinder", fillcolor=verizon_gray, fontcolor=verizon_black)
    dot.node("Table", "icm_summary_fact_exp", shape="box", fillcolor=verizon_white, fontcolor=verizon_black)

    # SQL Validation and Refinement
    dot.node("SQLValidator", "Validate & Execute SQL", fillcolor=verizon_white, fontcolor=verizon_black, color=verizon_red)
    dot.node("Error", "Error Message", shape="note", fillcolor="#ffcccc", fontcolor=verizon_black)
    dot.node("Refinement", "Iterative Refinement", fillcolor=verizon_white, fontcolor=verizon_black, color=verizon_red)

    # Result Processing
    dot.node("Results", "Query Results", shape="rectangle", fillcolor=verizon_white, fontcolor=verizon_black)
    dot.node("Format", "Format Results", fillcolor=verizon_white, fontcolor=verizon_black)
    dot.node("Response", "Present Response", shape="rectangle", fillcolor=verizon_white, fontcolor=verizon_dark_red, color=verizon_red)

    # --- Edges ---

    # User Interaction
    dot.edge("User", "Query")
    dot.edge("Query", "Agent")

    # NLU and Prompt
    dot.edge("Agent", "Intent")
    dot.edge("Agent", "Entities")
    dot.edge("Intent", "Mapping")
    dot.edge("Entities", "Mapping")
    dot.edge("Mapping", "Prompt")
    dot.edge("Agent", "Prompt")  # Agent also contributes to prompt generation

    # Gemini Model and Function Calls
    dot.edge("Prompt", "Gemini")
    dot.edge("Gemini", "GetSchema", label="Call if needed", style="dashed", color=verizon_dark_gray)
    dot.edge("Gemini", "ExecuteQuery", label="Call if needed", style="dashed", color=verizon_dark_gray)
    dot.edge("Gemini", "GetValues", label="Call if needed", style="dashed", color=verizon_dark_gray)

    # Function Call Implementations (BigQuery Interaction)
    dot.edge("GetSchema", "BigQuery", label="Get Schema", color=verizon_black)
    dot.edge("BigQuery", "GetSchema", label="Schema Info", color=verizon_black)
    dot.edge("ExecuteQuery", "BigQuery", label="Execute Query", color=verizon_black)
    dot.edge("BigQuery", "ExecuteQuery", label="Results/Error", color=verizon_black)
    dot.edge("GetValues", "BigQuery", label="Get Values", color=verizon_black)
    dot.edge("BigQuery", "GetValues", label="Values", color=verizon_black)

    # Database Table
    dot.edge("BigQuery", "Table")

    # SQL Validation and Refinement
    dot.edge("ExecuteQuery", "SQLValidator")
    dot.edge("SQLValidator", "Error", label="Error", color=verizon_red)
    dot.edge("Error", "Refinement")
    dot.edge("Refinement", "Prompt", label="Feedback", color=verizon_red)
    dot.edge("SQLValidator", "Results", label="Valid")

    # Result Processing
    dot.edge("Results", "Format")
    dot.edge("Format", "Response")
    dot.edge("Response", "User")

    # Add a legend
    with dot.subgraph(name="cluster_legend") as legend:
        legend.attr(label="Legend", style="filled", fillcolor="lightgrey", labeljust="l", fontsize="11", fontname="Arial", color=verizon_red)
        legend.node("l_user", "User", shape="ellipse", fillcolor=verizon_gray, fontcolor=verizon_black)
        legend.node("l_agent", "Gemini Agent", shape="diamond", fillcolor=verizon_white, fontcolor=verizon_dark_red)
        legend.node("l_nlu", "NLU Components", fillcolor=verizon_white, fontcolor=verizon_black)
        legend.node("l_prompt", "Prompt Engineering", fillcolor=verizon_white, fontcolor=verizon_black)
        legend.node("l_gemini", "Gemini 1.5 Pro", shape="cylinder", fillcolor=verizon_gray, fontcolor=verizon_black)
        legend.node("l_bq", "BigQuery Database", shape="cylinder", fillcolor=verizon_gray, fontcolor=verizon_black)
        legend.node("l_table", "BigQuery Table", shape="box", fillcolor=verizon_white, fontcolor=verizon_black)
        legend.node("l_func", "Function Call", shape="rectangle", fillcolor=verizon_white, fontcolor=verizon_black)
        legend.node("l_sqlval", "SQL Validation", fillcolor=verizon_white, fontcolor=verizon_black)
        legend.node("l_error", "Error Message", shape="note", fillcolor="#ffcccc", fontcolor=verizon_black)
        legend.node("l_result", "Result Processing", fillcolor=verizon_white, fontcolor=verizon_black)
        legend.edge("l_user", "l_agent", style="invis")
        legend.edge("l_agent", "l_nlu", style="invis")
        legend.edge("l_nlu", "l_prompt", style="invis")
        legend.edge("l_prompt", "l_gemini", style="invis")
        legend.edge("l_gemini", "l_func", style="invis")
        legend.edge("l_func", "l_bq", style="invis")
        legend.edge("l_bq", "l_table", style="invis")
        legend.edge("l_table", "l_sqlval", style="invis")
        legend.edge("l_sqlval", "l_result", style="invis")
        legend.edge("l_result", "l_error", style="invis")
        legend.edge("l_error", "l_user", style="invis")

    return dot

# Generate the diagram
dot = create_nl_to_sql_workflow_diagram()
dot.render("nl_to_sql_workflow_verizon", format="png", cleanup=True)

print("NL-to-SQL workflow diagram generated as nl_to_sql_workflow_verizon.png")
