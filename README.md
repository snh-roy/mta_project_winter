# Motivation

The Climate and Sustainability Team at the MTA relies on a decentralized and outdated approach to collecting precipitation records from 492 stations across New York City. This API solves that problem by centralizing real-time precipitation data into a single system. The API outputs the data in an Excel file for easy access.  

# MTA Rainfall API (Backend + Frontend)

This repo contains:
- **Backend** (FastAPI) at the repo root
- **Frontend** (Vite/React) in `frontend/`

---

## Quick Start (Local Demo)

### 1) Backend

```bash
cd mta-flood-api
cp .env.example .env
# Set your token in .env
# NCEI_CDO_TOKEN=...

pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Since this is FASTAPI, the Backend runs at: `http://127.0.0.1:8000` 

### 2) Frontend

```bash
cd mta-flood-api/frontend
npm install
npm run dev
```

Frontend runs at: `http://localhost:8080`

---

## Requirements

### Backend
- Python 3.10+
- `pip install -r requirements.txt`
- **NCEI CDO token** in `.env`
- **ECCODES** for GRIB decoding (MRMS/Stage IV):
  ```bash
  conda install -c conda-forge eccodes
  ```

### Frontend
- Node.js 18+
- `npm install`

---

## Data Sources

- **Current precipitation:** NOAA MRMS (rate, 1‑hour, 6‑hour)
- **Historical precipitation (2021‑present):** NCEP Stage IV hourly archive
- **Daily station totals:** NOAA NCEI CDO (Central Park, JFK, LGA) with fallback to last 7 days
- **Tides:** NOAA CO‑OPS (Battery + Kings Point)
- **Forecasts:** NWS Gridpoint API (today only)

---

## Notes / Limitations

- Historical precipitation depends on archive availability; some hours may be missing.
- Daily station totals can be missing for same‑day; fallback uses most recent available daily value.
- Full reports can take a few minutes (forecasts are the slowest step).

---

## Demo Script (2–4 minutes)

1) **Start backend**
```bash
cd mta-flood-api
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

2) **Start frontend**
```bash
cd mta-flood-api/frontend
npm run dev
```

3) **In the UI**
- Pick today’s date and a time
- Select a borough (e.g., Bronx)
- Click **Generate Excel**
- Open the file and show:
  - Station fields, precipitation, tide, daily CP/JFK/LGA totals
  - Risk Level + Risk Reason

4) **Historical example**
```
curl "http://127.0.0.1:8000/api/report?format=xlsx&date=2021-08-31&time=22:00" -o ida.xlsx
``