import pandas as pd
from typing import Tuple, Optional
import io

def validate_csv_file(file) -> Tuple[bool, str]:
    """Validate if uploaded file is a CSV and not empty."""
    if file is None:
        return False, "No file uploaded"
    
    if not file.name.endswith('.csv'):
        return False, "Please upload a CSV file"
    
    try:
        df = pd.read_csv(file)
        if df.empty:
            return False, "The uploaded file is empty"
        return True, "File is valid"
    except Exception as e:
        return False, f"Error reading file: {str(e)}"

def get_download_link(df: pd.DataFrame, filename: str) -> Tuple[io.BytesIO, str]:
    """Create a download link for a DataFrame."""
    towrite = io.BytesIO()
    df.to_csv(towrite, index=False)
    towrite.seek(0)
    return towrite, filename
