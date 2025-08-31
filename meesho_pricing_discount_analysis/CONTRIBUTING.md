# Contributing

Thanks for your interest in contributing!

## Getting Started
1. Create a virtualenv and install requirements:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. (Optional) Regenerate data:
   ```bash
   python data_gen.py
   ```

3. Run the app:
   ```bash
   streamlit run app.py
   ```

## Code Style
- Python: follow PEP8
- Keep functions small and testable
- Add helpful docstrings and comments

## Pull Requests
- Open a PR with a clear description and screenshots (if UI changes)
- Reference any issues it closes
