# Gutenberg Book Crawler

This project provides a command-line tool to download books from Project Gutenberg and store them in a local datalake. The crawler supports multiple modes for downloading books based on their IDs.

## Setup

To set up the project, you need to create and activate a Python virtual environment, then install the required dependencies.

### 1. Create a Virtual Environment

First, create a virtual environment in the project directory:

```bash
python -m venv venv
```

### 2. Activate the Virtual Environment

Activate the virtual environment using the appropriate command for your operating system:

- **Windows**:
  ```bash
  venv\Scripts\activate
  ```

- **MacOS/Linux**:
  ```bash
  source venv/bin/activate
  ```

### 3. Install Dependencies

Once the virtual environment is activated, install the required dependencies:

```bash
pip install -r requirements.txt
```

## Running the Crawler

To run the crawler, execute the following command from the project's root directory:

```bash
python -m crawler.cli
```

The crawler supports the following mutually exclusive options:

- `--id <ID>`: Download a single book by its Gutenberg ID (e.g., `python -m crawler.cli --id 1342`).
- `--range <START> <END>`: Download a range of books inclusively from `START` to `END` (e.g., `python -m crawler.cli --range 100 110`).
- `--list <FILE>`: Download books listed in a file, with one ID per line (e.g., `python -m crawler.cli --list ids.txt`).
- `--continuous`: Download books continuously in batches of 10, starting from ID 1, with a 4-minute pause between batches. Stops after 10 consecutive failures (e.g., `python -m crawler.cli --continuous`).

## Output

The crawler saves downloaded books to a datalake directory, with each book split into header and body text files. It prints the status of each download (`[OK]` or `[FAIL]`) and a summary of successful (`OK`) and failed (`FAIL`) downloads at the end.