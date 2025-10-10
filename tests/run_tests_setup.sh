#!/bin/bash
# Script to set up test environment and run tests
# This demonstrates best practices for running PySpark tests locally

set -e  # Exit on error

echo "======================================"
echo "IKEA Lakehouse: Test Setup & Execution"
echo "======================================"
echo ""

# 1. Create virtual environment
echo "Step 1: Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# 2. Activate virtual environment
echo ""
echo "Step 2: Activating virtual environment..."
source venv/bin/activate
echo "✓ Virtual environment activated"

# 3. Install dependencies
echo ""
echo "Step 3: Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "✓ Dependencies installed"

# 4. Verify installations
echo ""
echo "Step 4: Verifying installations..."
python3 -c "import pytest; print(f'✓ pytest {pytest.__version__}')"
python3 -c "import chispa; print('✓ chispa installed')"
python3 -c "import pyspark; print(f'✓ pyspark {pyspark.__version__}')"

# 5. Run tests
echo ""
echo "Step 5: Running tests..."
echo "======================================="
pytest tests/test_transforms.py -v

# 6. Show summary
echo ""
echo "======================================"
echo "Test execution complete!"
echo "======================================"
echo ""
echo "Next steps:"
echo "  - Review test results above"
echo "  - Check coverage with: pytest tests/test_transforms.py -v --cov=tests"
echo "  - Run specific tests with: pytest tests/test_transforms.py::TestFXTransform -v"
echo ""
echo "To deactivate virtual environment, run: deactivate"

