## Snowflake Postgres Transactional Workload Demo

This project is a small, end-to-end example of a **transactional (OLTP) workload** running on **Snowflake Postgres**, built to extend the official getting started guide:  
**“Getting Started with Snowflake Postgres”** (`https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/`).  
That guide covers instance creation and basic SQL; this project adds a workload generator and a Streamlit dashboard on top.

It creates a simple e-commerce schema (`customers`, `products`, `orders`, `inventory`, `payments`), a transactional `place_order` stored function, continuous ingestion, and KPI dashboards backed by Postgres functions.

**Highlights**

- **Aligned with Snowflake docs**: Follows the official [Getting Started with Snowflake Postgres](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/) flow for instance creation and basic SQL.
- **End-to-end OLTP demo**: From schema creation and transactional workloads to continuous ingestion and analytics.
- **UI + backend separation**: All KPI logic lives in Postgres functions; Streamlit is a thin, Snowflake-themed UI layer.
- **Safe to re-run**: Idempotent seed scripts and initialization so you can iterate without resetting the instance.

---

### 1. Prerequisites

From the Snowflake guide (`https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/`):

- **Snowflake account or trial**.
- `ACCOUNTADMIN` role or a role with `CREATE POSTGRES INSTANCE`.
- Ability to attach Postgres to a network (a network policy that allows your IP).
- A local Postgres client (e.g. `psql`) or GUI (DBeaver, DataGrip, pgAdmin, etc.).

Additional for this project:

- Python **3.9+** on your machine.
- Network access from your machine to the Snowflake Postgres instance (your IP must be allowed by the network policy).

> **Security note**: Never commit real passwords or secrets into git. Use environment variables instead.

---

### 2. Create a Snowflake Postgres instance (Snowsight)

Follow the “Deploy a Postgres Instance” section of the Snowflake guide (`https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-postgres/`):

1. In **Snowsight**, click the **+** button in the nav or go to the **Manage** section and choose **Postgres**.
2. Configure the instance:
   - **Name**: something meaningful, e.g. `dev-test` or `ecomm-demo`.
   - **Instance class / memory**: for this demo, a small burstable instance (e.g. 2 cores, 2 GB) is sufficient.
   - **Storage**: for tests, the default (e.g. 10 GB) is enough.
   - **Postgres version**: choose a supported version; newest is fine if you don’t have a preference.
   - **Network policy**: either:
     - Attach an existing policy that allows your machine’s public IP, or
     - Create a new policy and add your IP (see the guide’s network policy notes).
3. Create the instance and wait for it to reach a **RUNNING** state.

This is the same flow described in the Snowflake guide’s “Deploy a Postgres Instance” section.

---

### 3. Get connection details from Snowsight

Once the instance is created, Snowsight shows a **connection screen** (see “Connect to Postgres” in the guide):

- You’ll see:
  - **Host name** (similar to `xxxx.sf<region>-<account>.aws.postgres.snowflake.app`)
  - **Port**: `5432`
  - **Username**: `snowflake_admin` (and optionally an `application` user)
  - **Password**: a generated password you should store securely
  - **Database**: defaults to `postgres` unless you create another
- You can copy either:
  - A full **connection string** (e.g. `postgres://snowflake_admin:***@HOST:5432/postgres`), or
  - Individual fields to export as environment variables.

For this project, we use environment variables:

```bash
export PGHOST="<YOUR_SNOWFLAKE_POSTGRES_HOSTNAME>"
export PGPORT="5432"
export PGUSER="snowflake_admin"
export PGPASSWORD="<YOUR_SECURE_PASSWORD>"
export PGDATABASE="postgres"
```

Replace `<YOUR_SNOWFLAKE_POSTGRES_HOSTNAME>` and `<YOUR_SECURE_PASSWORD>` with the values from the connection screen.

You can test with `psql` as suggested in the guide:

```bash
psql "postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE"
```

If this connects successfully, you’re ready to run the rest of this project.

---

### 4. Install dependencies (local project setup)

From the project root (`/Users/bsuresh/Documents/postgres`):

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

Dependencies include:

- `psycopg2-binary` – Postgres driver.
- `streamlit`, `pandas`, `altair` – UI and charting.
- `python-dotenv` – optional env-var helpers.

---

### 5. Initialize the database (schema, seed, functions)

This step:

- Creates tables (`customers`, `products`, `orders`, `inventory`, `payments`).
- Inserts sample customers and products (as in the Snowflake guide, plus inventory).
- Seeds initial inventory (idempotent).
- Creates:
  - `place_order` transactional function.
  - KPI helper functions (`fn_summary_kpis`, `fn_sales_by_category`, etc.) used by the Streamlit app.

From the project root:

```bash
source .venv/bin/activate
python -m app.init_db
```

You should see log lines as each SQL file is applied:

- `01_schema.sql`
- `02_seed_data.sql`
- `03_functions.sql`
- `04_kpi_functions.sql`

You can also verify the basic tables and queries in `psql` exactly as shown in the Snowflake guide.

---

### 6. Run the transactional workload

The workload script uses multiple threads to call the `place_order` stored function concurrently, simulating many small OLTP transactions (checkouts).

```bash
source .venv/bin/activate

python -m app.run_workload \
  --workers 4 \
  --orders-per-worker 250 \
  --max-customers 10 \
  --max-products 10
```

Parameters:

- **`--workers`**: Number of concurrent worker threads.
- **`--orders-per-worker`**: How many orders each worker submits.
- **`--max-customers`**: Upper bound for `customer_id` to use (1..N).
- **`--max-products`**: Upper bound for `product_id` to use (1..N).

The script prints:

- Total elapsed time.
- Number of successful and failed orders.
- Approximate throughput (successful orders per second).

---

### 7. Continuous ingestion every N seconds

To keep producing fresh data into your Snowflake Postgres instance (for the live KPIs), run the continuous ingestion script. By default it ingests a small batch every 20 seconds.

```bash
source .venv/bin/activate

python -m app.continuous_ingest \
  --interval-seconds 20 \
  --batch-size 10 \
  --max-customers 10 \
  --max-products 10
```

This will run until you stop it with `Ctrl+C`.

---

### 8. Inspect the results from the CLI (optional)

You can still use simple CLI-based queries, similar to the examples in the Snowflake guide:

```bash
source .venv/bin/activate
python -m app.queries
```

This will output:

- **Inventory snapshot** for each product.
- **Top customers by total spend**.
- **Sales by product category**.

You can also connect directly with `psql`:

```bash
psql "postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE"
```

and run any SQL from the official guide (joins, aggregates, etc.) or your own statements.

---

### 9. Real-time KPIs with Streamlit

You can visualize live KPIs in a Streamlit app while ingestion is running. The UI calls Postgres functions (`fn_summary_kpis`, `fn_sales_by_category`, `fn_inventory_snapshot`, `fn_recent_orders`, `fn_revenue_timeseries`, `fn_top_customers`) so all logic runs in Snowflake Postgres.

```bash
source .venv/bin/activate
streamlit run streamlit_app.py
```

Open the URL Streamlit prints (typically `http://localhost:8501`) and you’ll see:

- **Summary metrics**:
  - Total orders and revenue (all time).
  - Orders, revenue, active customers, average order value, and orders/min in the last N minutes.
- **Interactive charts**:
  - Sales by category (last N minutes).
  - Inventory snapshot by product.
  - Most recent orders (last N minutes).
  - Revenue & orders over time (per minute).
  - Top customers by revenue.
- **Controls**:
  - Time window slider (defines “recent”).
  - Recent orders limit.
  - Category filter (applies to charts and window-based KPIs).
  - Refresh buttons.

Leave `app.continuous_ingest` running in one terminal and the Streamlit app in another to see KPIs and charts evolve as new transactions arrive.

---

### 10. Project structure

- **`requirements.txt`**: Python dependencies.
- **`sql/01_schema.sql`**: Table definitions and indexes.
- **`sql/02_seed_data.sql`**: Sample customers, products, and initial inventory.
- **`sql/03_functions.sql`**: `place_order` stored function and transactional logic.
- **`sql/04_kpi_functions.sql`**: KPI helper functions used by the Streamlit app.
- **`app/__init__.py`**: Marks `app` as a package.
- **`app/db.py`**: Connection helper and cursor context manager.
- **`app/init_db.py`**: Runs all SQL files to initialize the database.
- **`app/run_workload.py`**: Multithreaded transactional workload driver.
- **`app/queries.py`**: Convenience analytics to inspect results from the CLI.
- **`app/continuous_ingest.py`**: Continuous ingestion of orders every N seconds.
- **`streamlit_app.py`**: Streamlit dashboard for live KPIs, backed by Postgres functions.

This gives you a complete path:

1. Follow the **Snowflake guide** to create and connect to a Snowflake Postgres instance.  
2. Point this project at that instance via environment variables.  
3. Initialize, generate transactional load, and explore live KPIs using the Python tools in this repo.
