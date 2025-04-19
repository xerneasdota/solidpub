#!/usr/bin/env python3
"""
Script to fix relative imports in the Binance Trading Analysis System.
This script will find and replace all relative imports (..module) with absolute imports.
"""
import os
import re
import sys
from pathlib import Path

def fix_file(file_path):
    """Fix relative imports in a single file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find all relative imports with .. or ...
    relative_import_pattern = r'from\s+\.\.([a-zA-Z0-9_.]+)\s+import'
    matches = re.findall(relative_import_pattern, content)
    
    if not matches:
        print(f"No relative imports found in {file_path}")
        return False
    
    print(f"Found {len(matches)} relative imports in {file_path}")
    
    # Replace relative imports with absolute imports
    modified_content = content
    for module in matches:
        module_name = module.strip('.')
        if not module_name:
            continue
        
        # Replace relative import with absolute import
        relative_import = f"from ..{module_name} import"
        absolute_import = f"# Fix relative import\nimport sys\nimport os\nsys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))\nfrom {module_name} import"
        
        # Only add the fix once
        if "# Fix relative import" not in modified_content:
            modified_content = modified_content.replace(relative_import, absolute_import)
    
    # Write the modified content back to the file
    if modified_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(modified_content)
        print(f"Fixed imports in {file_path}")
        return True
    
    return False

def find_python_files(directory):
    """Find all Python files in a directory and its subdirectories."""
    python_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    return python_files

def main():
    # Get the project directory
    project_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Find all Python files
    python_files = find_python_files(project_dir)
    print(f"Found {len(python_files)} Python files")
    
    # Fix imports in each file
    fixed_files = 0
    for file_path in python_files:
        if fix_file(file_path):
            fixed_files += 1
    
    print(f"Fixed imports in {fixed_files} files")

if __name__ == "__main__":
    main()
