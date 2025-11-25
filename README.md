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

### 1. Google Cloud Setup (Required Before Running)
Before using the notebook, make sure your GCP project has the following:

**Enable Billing**  
Your project must have billing or free credits enabled.

**Enable Required APIs**  
- Compute Engine API  
- Dataproc API  
- Cloud Storage API  

**Allow Compute Engine / Dataproc to Access Storage**  
When creating your Dataproc cluster or opening a Serverless Notebook:  
- Make sure the notebook’s service account has the following IAM roles:  
  - Storage Object Viewer  
  - Storage Object Admin (if you want to write output files)  
  - Compute Viewer  
- When prompted, choose **“Allow full access to all Cloud APIs”** (or the standard Dataproc notebook default).

This allows the notebook to:  
- Download the dataset  
- Save output files  
- Use Dask / clusters  

**Open Dataproc JupyterLab**  
- In the GCP console, open **Dataproc → Serverless → Workbench** (the blue “chip” icon notebook environment)  
- Launch a new Python notebook environment  

### 2. Upload the Notebook
Upload `Co_Watched_Film_Rules.ipynb` to your Google Cloud environment  
(e.g., Dataproc JupyterLab, AI Platform Notebooks, or Vertex Workbench).

### 3. Run the Notebook *from top to bottom*
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

