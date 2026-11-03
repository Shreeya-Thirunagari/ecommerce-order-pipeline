# Windows Setup and Debugging History

## Development Environment

The project is intended to run on Windows using Python 3.10 or later.

## Setup Steps

### Step 1: Install Python

Install Python 3.10+ and verify:

```bash
python --version
```

### Step 2: Open the Project Folder

Open Command Prompt or PowerShell and navigate to the downloaded project directory:

```powershell
cd path\to\ecommerce-order-pipeline
```

### Step 3: Create a Virtual Environment

```bash
python -m venv .venv
```

### Step 4: Activate the Environment

PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Command Prompt:

```cmd
.venv\Scripts\activate.bat
```

### Step 5: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 6: Generate Sample Data

```bash
python scripts/generate_sample_data.py
```

### Step 7: Execute the Pipeline

Use the configuration filename actually present in the repository:

```bash
python -m src.pipeline --config config.yaml
```

### Step 8: Run Automated Tests

```bash
python -m pytest -v
```

## Common Troubleshooting

### Python Not Recognized

Check whether Python is installed and added to the Windows PATH.

### Module Not Found

Activate the virtual environment and install dependencies again:

```bash
pip install -r requirements.txt
```

### Permission Errors

Check folder permissions and ensure files are not open in another application.

### PowerShell Activation Error

If script execution is blocked, use Command Prompt and activate the environment with:

```cmd
.venv\Scripts\activate.bat
```

### Missing Configuration File

Verify that the expected configuration file exists in the project root and that the pipeline uses the correct filename.

## Execution Verification

After running the pipeline, inspect the following folders:

- `data/raw/`
- `data/validated/`
- `data/quarantine/`
- `data/curated/`
- `data/metrics/`

Run the tests and review their actual output before reporting successful execution.
