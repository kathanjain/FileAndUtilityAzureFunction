# File And Utility Azure Function

This project contains a Python-based Azure Function app with a sample HTTP trigger and a folder for Power Platform solution uploads.

## Structure
- `function_app.py`: Main Azure Function app with HTTP trigger.
- `requirements.txt`: Python dependencies for Azure Functions.
- `power_platform_solution/`: Folder to upload Power Platform solution files (e.g., .zip).
- `host.json` and `local.settings.json`: Required Azure Functions configuration files.
- `venv/`: Python virtual environment (not pushed to GitHub).

## Running Locally
1. Activate the virtual environment:
   ```
   venv\Scripts\activate
   ```
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Start the Azure Function:
   ```
   func start
   ```

## Deployment
Push this repository to GitHub and deploy to Azure using the Azure portal or CLI.
