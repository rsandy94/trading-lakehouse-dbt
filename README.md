Welcome to your new dbt project!

## Project setup (new system)

1. Install **Python 3.12** (the project was built on 3.12.10).
2. Create and activate a virtual environment:
   ```bash
   python -m venv .dbtenv
   .dbtenv\Scripts\activate       # Windows (PowerShell: .dbtenv\Scripts\Activate.ps1)
   source .dbtenv/bin/activate    # macOS / Linux
   ```
3. Install pinned dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Install dbt packages (dbt_utils):
   ```bash
   dbt deps
   ```
5. Configure your Snowflake connection in `~/.dbt/profiles.yml` using the profile name `trading_lakehouse_dbt` (credentials are not stored in this repo).
6. Build and test:
   ```bash
   dbt run
   dbt test
   ```

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
