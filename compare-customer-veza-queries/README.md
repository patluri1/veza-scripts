# Compare Customer Queries with Veza Master Queries

This project compares customer queries with Veza master queries and outputs the differences to CSV files.

## Prerequisites

- Python 3.x
- `pip` (Python package installer)

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/your-username/veza-scripts.git
    cd veza-scripts/compare-customer-veza-queries
    ```

2. Create a virtual environment (optional but recommended):

    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. Install the required packages:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. Ensure you have the `VEZA_COOKIE` environment variable set:

    ```sh
    export VEZA_COOKIE=your_veza_cookie  # On Windows, use `set VEZA_COOKIE=your_veza_cookie`
    ```

2. Run the script:

    ```sh
    python compare_customer_queries_with_veza_master_queries.py --host your_host_name --csv path_to_your_csv_file --assess_against path_to_master_queries_json_file
    ```

## Arguments

- `--host`: Veza host name.
- `--csv`: Path to the CSV file containing the query names.
- `--assess_against`: Path to the master queries JSON file.

## Output

The script will create an `output` directory and write the differences to individual CSV files for each query.

## Cleanup

To clean up the output directory, you can use the `cleanup` function:

```python
from compare_customer_queries_with_veza_master_queries import cleanup
cleanup()