# Databricks notebook source
# MAGIC %md
# MAGIC ## 13_generate_join_diagram
# MAGIC 
# MAGIC **Purpose**: Generate visual join lineage diagram from `config/joins.yml`
# MAGIC 
# MAGIC **Outputs**:
# MAGIC - Mermaid diagram code (for documentation)
# MAGIC - HTML with embedded Mermaid (for Lakeview/dashboards)
# MAGIC - PNG export (via Mermaid CLI, optional)
# MAGIC 
# MAGIC **Why this matters**: Proves metadata-driven architecture is self-documenting

# COMMAND ----------

%pip install pyyaml

# COMMAND ----------

import yaml
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Join Configuration from YAML

# COMMAND ----------

# Resolve path to config/joins.yml
def resolve_repo_root():
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        try:
            nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            workspace_path = Path("/Workspace") / nb_path.lstrip("/")
            return workspace_path.parents[1]
        except:
            return Path.cwd().resolve()

repo_root = resolve_repo_root()
config_path = repo_root / "config" / "joins.yml"

print(f"📂 Loading config from: {config_path}")

# Read YAML
if 'dbutils' in globals():
    try:
        config_content = dbutils.fs.head(f"dbfs:{config_path.as_posix()}", 10000)
    except:
        # Fallback for local file
        with open(config_path) as f:
            config_content = f.read()
else:
    with open(config_path) as f:
        config_content = f.read()

config = yaml.safe_load(config_content)
print("✓ Config loaded successfully\n")
print(yaml.dump(config, default_flow_style=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Generate Mermaid Diagram Code

# COMMAND ----------

def generate_mermaid_diagram(config):
    """
    Generate Mermaid flowchart from join config.
    
    Syntax:
      graph LR
        base[silver.baskets] --> cust[silver.dim_customers_api]
        base --> fx[silver.fx_rates_daily]
        cust --> output[gold.baskets_enriched]
        fx --> output
    """
    
    base_cfg = config['base']
    base_table = base_cfg['table']
    base_alias = base_cfg.get('alias', 'base')
    
    output_cfg = config.get('output', {})
    output_table = output_cfg.get('table', 'gold.output')
    
    # Start diagram
    lines = ["graph LR"]
    lines.append(f"  {base_alias}[{base_table}]")
    
    # Add join nodes
    for idx, join_cfg in enumerate(config.get('joins', []), 1):
        join_table = join_cfg['table']
        join_alias = join_cfg['alias']
        join_type = join_cfg.get('type', 'left').upper()
        is_broadcast = join_cfg.get('broadcast', False)
        
        # Add join node
        if is_broadcast:
            lines.append(f"  {join_alias}[{join_table}<br/>BROADCAST]:::broadcast")
        else:
            lines.append(f"  {join_alias}[{join_table}]")
        
        # Add edge from base to join dimension
        edge_label = f"{join_type} JOIN"
        lines.append(f"  {base_alias} -->|{edge_label}| {join_alias}")
    
    # Add output node
    lines.append(f"  output[{output_table}]:::output")
    
    # Connect all joins to output
    lines.append(f"  {base_alias} --> output")
    for join_cfg in config.get('joins', []):
        join_alias = join_cfg['alias']
        lines.append(f"  {join_alias} --> output")
    
    # Add styles
    lines.append("")
    lines.append("  classDef broadcast fill:#e1f5dd,stroke:#4caf50,stroke-width:2px")
    lines.append("  classDef output fill:#fff3cd,stroke:#ff9800,stroke-width:3px")
    
    return "\n".join(lines)

mermaid_code = generate_mermaid_diagram(config)

print("📊 GENERATED MERMAID DIAGRAM:")
print("="*70)
print(mermaid_code)
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Generate HTML with Embedded Mermaid

# COMMAND ----------

def generate_html(mermaid_code, title="Join Lineage Diagram"):
    """Generate standalone HTML with Mermaid.js rendering."""
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
  <title>{title}</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <style>
    body {{
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      margin: 20px;
      background: #f5f5f5;
    }}
    h1 {{
      color: #333;
    }}
    .mermaid {{
      background: white;
      padding: 20px;
      border-radius: 8px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.1);
      margin: 20px 0;
    }}
    .info {{
      background: #e3f2fd;
      padding: 15px;
      border-left: 4px solid #2196F3;
      margin: 20px 0;
      border-radius: 4px;
    }}
    .legend {{
      background: #fff;
      padding: 15px;
      border-radius: 8px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.1);
      margin: 20px 0;
    }}
    .legend-item {{
      margin: 8px 0;
    }}
    .broadcast-box {{
      display: inline-block;
      background: #e1f5dd;
      border: 2px solid #4caf50;
      padding: 4px 12px;
      border-radius: 4px;
      margin-right: 10px;
    }}
    .output-box {{
      display: inline-block;
      background: #fff3cd;
      border: 3px solid #ff9800;
      padding: 4px 12px;
      border-radius: 4px;
      margin-right: 10px;
    }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  
  <div class="info">
    <strong>📊 Metadata-Driven Architecture</strong><br/>
    This diagram is auto-generated from <code>config/joins.yml</code>. 
    All joins are config-driven—add new tables by editing YAML, not code.
  </div>
  
  <div class="mermaid">
{mermaid_code}
  </div>
  
  <div class="legend">
    <strong>Legend:</strong>
    <div class="legend-item">
      <span class="broadcast-box">Table Name BROADCAST</span> = Small dimension, broadcast join hint
    </div>
    <div class="legend-item">
      <span class="output-box">Table Name</span> = Final output table (gold layer)
    </div>
    <div class="legend-item">
      <strong>→ LEFT JOIN</strong> = Join type label on edge
    </div>
  </div>
  
  <h2>Join Configuration Details</h2>
  <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; background: white;">
    <tr style="background: #2196F3; color: white;">
      <th>Alias</th>
      <th>Table</th>
      <th>Join Type</th>
      <th>On Condition</th>
      <th>Broadcast</th>
    </tr>
"""
    
    # Add base row
    base_cfg = config['base']
    html += f"""
    <tr style="background: #f5f5f5;">
      <td><strong>{base_cfg.get('alias', 'base')}</strong></td>
      <td>{base_cfg['table']}</td>
      <td>BASE TABLE</td>
      <td>-</td>
      <td>-</td>
    </tr>
"""
    
    # Add join rows
    for join_cfg in config.get('joins', []):
        broadcast_emoji = "✅" if join_cfg.get('broadcast') else "❌"
        html += f"""
    <tr>
      <td>{join_cfg['alias']}</td>
      <td>{join_cfg['table']}</td>
      <td>{join_cfg.get('type', 'left').upper()}</td>
      <td><code>{join_cfg.get('on', '')}</code></td>
      <td>{broadcast_emoji}</td>
    </tr>
"""
    
    html += """
  </table>
  
  <script>
    mermaid.initialize({ startOnLoad: true, theme: 'default' });
  </script>
</body>
</html>
"""
    
    return html

html_output = generate_html(mermaid_code)

print("✓ HTML generated")
print(f"   Length: {len(html_output)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Save Outputs

# COMMAND ----------

# Save Mermaid code to DBFS for documentation
mermaid_path = repo_root / "docs" / "join_diagram.mmd"
html_path = repo_root / "docs" / "join_diagram.html"

print("💾 Saving outputs...")

# Write Mermaid code
with open(mermaid_path, "w") as f:
    f.write(mermaid_code)
print(f"✓ Saved: {mermaid_path}")

# Write HTML
with open(html_path, "w") as f:
    f.write(html_output)
print(f"✓ Saved: {html_path}")

print("\n📋 HOW TO USE:")
print("1. Open join_diagram.html in a browser to see interactive diagram")
print("2. Copy join_diagram.mmd content to GitHub/Confluence (supports Mermaid)")
print("3. Use HTML in Lakeview as HTML iframe widget")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Display Diagram in Notebook

# COMMAND ----------

# Render Mermaid in Databricks notebook using HTML widget
displayHTML(html_output)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Generate Markdown Documentation

# COMMAND ----------

def generate_markdown_doc(config, mermaid_code):
    """Generate Markdown documentation with embedded Mermaid."""
    
    md = f"""# Join Lineage Documentation

## Overview

This document describes the metadata-driven join configuration for the IKEA Lakehouse gold layer.

**Base Table**: `{config['base']['table']}`  
**Output Table**: `{config.get('output', {}).get('table', 'gold.output')}`  
**Join Strategy**: Config-driven (YAML)

## Join Diagram

```mermaid
{mermaid_code}
```

## Join Specifications

"""
    
    for idx, join_cfg in enumerate(config.get('joins', []), 1):
        md += f"""### Join {idx}: {join_cfg['table']}

- **Alias**: `{join_cfg['alias']}`
- **Join Type**: `{join_cfg.get('type', 'left').upper()}`
- **On Condition**: `{join_cfg.get('on', '')}`
- **Broadcast**: {'✅ Yes (small dimension)' if join_cfg.get('broadcast') else '❌ No'}
- **Selected Columns**: {', '.join(f"`{c}`" for c in join_cfg.get('select', []))}

"""
    
    md += f"""## Configuration File

**Source**: `config/joins.yml`

```yaml
{yaml.dump(config, default_flow_style=False)}
```

## How to Update

To add a new join:

1. Edit `config/joins.yml`
2. Add new entry to `joins` list:
   ```yaml
   - table: silver.new_dimension
     alias: new_dim
     type: left
     "on": "base.key_field = new_dim.id"
     select:
       - field1
       - field2
     broadcast: true  # if small dimension
   ```
3. Re-run `09_metadata_joins.py`
4. Re-run `13_generate_join_diagram.py` to update this diagram

**No code changes required** — just update YAML!

---

*Auto-generated by `13_generate_join_diagram.py`*  
*Last updated: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}*
"""
    
    return md

import datetime

markdown_doc = generate_markdown_doc(config, mermaid_code)

# Save markdown
md_path = repo_root / "docs" / "JOIN_LINEAGE.md"
with open(md_path, "w") as f:
    f.write(markdown_doc)

print(f"✓ Saved: {md_path}")
print("\n📖 Markdown documentation generated with embedded Mermaid")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary
# MAGIC 
# MAGIC ✅ **Generated Artifacts**:
# MAGIC - `docs/join_diagram.mmd` - Mermaid source code
# MAGIC - `docs/join_diagram.html` - Standalone HTML with interactive diagram
# MAGIC - `docs/JOIN_LINEAGE.md` - Full documentation with embedded Mermaid
# MAGIC 
# MAGIC ✅ **Use Cases**:
# MAGIC - **Lakeview**: Embed HTML as iframe widget in dashboard
# MAGIC - **Documentation**: Copy Markdown to Confluence/GitHub wiki
# MAGIC - **Presentations**: Open HTML in browser, screenshot for slides
# MAGIC - **Self-documenting architecture**: Diagram auto-updates with YAML changes
# MAGIC 
# MAGIC **What this proves**: Metadata-driven architecture is maintainable, scalable, and self-documenting.

