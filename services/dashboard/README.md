# Pizza Pulse Dashboard

Streamlit dashboard for the online serving tables populated by Spark Streaming.

It reads PostgreSQL tables:

- `demand_predictions`
- `ingredient_risk_predictions`

Run locally after port-forwarding PostgreSQL:

```bash
python -m venv .venv-dashboard
. .venv-dashboard/bin/activate
pip install -r services/dashboard/requirements.txt

POSTGRES_HOST=localhost \
POSTGRES_PASSWORD=admin \
streamlit run services/dashboard/app.py --server.port 8501
```

Build the image:

```bash
docker build -t thaihoc285/pp-dashboard:0.0.1 services/dashboard
docker push thaihoc285/pp-dashboard:0.0.1
```
