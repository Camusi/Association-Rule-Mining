# Co-Watched TV/Film Association Rule Mining

## Overview
This project uses the **Apriori algorithm** to discover **co-watched viewing patterns** from a large public movie/TV ratings dataset.  
All analysis is performed inside a single Jupyter Notebook:

**`Co_Watched_Film_Rules.ipynb`**

This notebook:
- Automatically **downloads the dataset** from its original source  
- **Preprocesses** the data into user “baskets”
- **Generates frequent itemsets**
- **Produces association rules**
- **Exports results**

---

## How to Run (Google Cloud)

### 1. Upload the Notebook
Upload `Co_Watched_Film_Rules.ipynb` to your Google Cloud environment  
(e.g., Dataproc JupyterLab, AI Platform Notebooks, or Vertex Workbench).

### 2. Run the Notebook *from top to bottom*
The cells are ordered so that the entire workflow executes correctly when run sequentially.

### The Notebook Automatically:
- Downloads the dataset  
- Converts and preprocesses it  
- Creates Dask clusters (when needed)  
- Runs Apriori  
- Generates association rules  
- Saves results to output files

---

## Notes
- Exported results (e.g., CSV or JSON) are optional and created by the notebook.  
- Running on Google Cloud is recommended due to dataset size and intensive computation.

