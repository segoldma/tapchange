# 🍺 tapchange - Beer List Tracker

TapChange automatically monitors changes to a local bar's beer list and tracks additions, and removals. The project reads from a public Google Sheet, detects daily changes, and analyzes them with DuckDB.

## 📊 What It Does

- **Daily Monitoring**: Automatically checks the beer list every day at noon EST
- **Change Detection**: Identifies new beers added and removed from the tap list
- **Historical Tracking**: Maintains a complete history of beer availability in a DuckDB database

## 🏗️ Orchestration

The project uses **Dagster assets** for data pipeline orchestration, even though scheduling is handled by GitHub Actions. This is because I want to learn more about Dagster, and make it easier to scale up to more complex workflows (e.g., alerting me if my favorite brewery is added to the list)

### Data Pipeline Flow

```
Google Sheet → current_beer_list → beer_list_snapshot → beer_list_changes
```

1. **`current_beer_list`**: Fetches and parses the Google Sheet CSV export
2. **`beer_list_snapshot`**: Creates timestamped snapshot with normalized data
3. **`beer_list_changes`**: Compares with previous snapshot to detect additions/removals

## 🚀 Getting Started

### Local Development

```bash
# Clone the repository
git clone <your-repo-url>
cd tapchange

# Set up environment
python -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run manually
python run_tracker.py

# Or use Dagster UI for development
dagster dev
```

### View Results

```bash
# Preview latest beer list and changes
python view_data.py

# Query database directly
duckdb beer_tracking.duckdb
```

## 🤖 Automation

The project runs automatically via **GitHub Actions**:

- **Schedule**: Daily at noon EST (17:00 UTC)
- **Trigger**: Manual execution available in GitHub Actions tab
- **Persistence**: Results committed back to repository in `beer_tracking.duckdb`
- **Monitoring**: Check the Actions tab for run history and logs

## 📁 Project Structure

```
tapchange/
├── .github/workflows/
│   └── beer-tracker.yml      # GitHub Actions workflow
├── tapchange/
│   ├── assets.py             # Dagster data pipeline assets
│   ├── jobs.py               # Asset job definitions
│   ├── schedules.py          # Schedule definitions (for local dev)
│   └── definitions.py        # Dagster definitions
├── run_tracker.py            # Standalone runner for GitHub Actions
├── view_data.py              # Data viewing script
├── requirements.txt          # Python dependencies
└── beer_tracking.duckdb      # Database
```

## 🛠️ Technology Stack

- **[DuckDB](https://duckdb.org/)**: Embedded analytics database with CSV reading capabilities
- **[Dagster](https://dagster.io/)**: Data pipeline orchestration and asset management
- **[GitHub Actions](https://github.com/features/actions)**: Free scheduling and automation
- **Python**: Core application logic

## 🔄 Data Schema

### Beer Records
```json
{
  "vessel": "Draft" | "Bottle/Can",
  "name": "Beer Name",
  "style": "IPA, Lager, etc.",
  "brewery_location": "City, State",
  "abv": "5.5%",
  "description": "Tasting notes and details"
}
```