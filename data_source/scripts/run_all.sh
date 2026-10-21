#!/usr/bin/env bash
set -euo pipefail

# echo "==> Installing deps"
# pip install -r requirements.txt

echo "==> Generating 3PL CSVs"
python -m threepl_generator.generate

echo "==> Seeding Supabase POS database"
python -m pos_supabase.02_seed

echo "==> Starting mock API (background)"
( cd mock_api && uvicorn app:app --host 0.0.0.0 --port 8080 ) &
API_PID=$!
sleep 3

echo "==> Sanity testing API"
python scripts/test_api.py || true

echo "==> All sources ready. Kill mock API with: kill $API_PID"
wait $API_PID