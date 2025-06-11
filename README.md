# 🏋️ FoxRiverAI — AI-Driven Horse Racing Prediction System

FoxRiverAI is a machine learning platform for intelligent horse racing predictions using historical and real-time data. Built on top of GPS-based sectional timing, past performance data, and custom-engineered features, this system aims to power exotic wager strategies like Exactas, Trifectas, and Pick 3/4s.

> 📈 Designed to convert elite predictive modeling techniques into profitable, real-world wagering outcomes.

---

## ✅ Key Features

* 🧠 Predictive modeling with LSTM, CatBoost, and ensemble methods
* ⏳ Time-series analysis of real-time GPS and sectional data (via TPD)
* 📊 Past performance (PP) data ingestion from Equibase Plus Pro and Results
* 📍 PostGIS integration for spatial race dynamics
* 🔬 Feature engineering: fatigue, acceleration, normalized pace, class changes
* 🏇 Exotic bet optimization (Exacta, Pick 3/4, Superfecta)
* 📂 PostgreSQL backend with historical + live ingestion tracking
* 📀 Modular ingestion for TPD (GPS, sectionals, routes) and Equibase (XML)

---

## 📊 Tech Stack

| Layer         | Stack                                                                   |
| ------------- | ----------------------------------------------------------------------- |
| Core Language | Python (3.10+)                                                          |
| ML Models     | LSTM (Keras), CatBoost, XGBoost, Logistic Regression                    |
| Data Pipeline | Pandas, NumPy, xml.etree, JSON, Postgres COPY, LangChain (experimental) |
| Database      | PostgreSQL + PostGIS                                                    |
| Ingestion     | XML/JSON loaders for Equibase + TPD                                     |
| Viz (WIP)     | Matplotlib, Plotly, GeoJSON (future for track maps)                     |

---

## 📁 Project Layout

```
foxriver-ai/
├── data/                   # TPD + Equibase source files (structured folders)
├── models/                 # Training scripts and saved models
├── database/               # SQL schema and migration scripts
├── notebooks/              # EDA and model diagnostics
├── ingestion/              # XML/JSON processors and job runners
├── features/               # Feature engineering modules
├── betting/                # Simulated bet strategy execution + ROI logging
├── config/                 # YAML settings and environment variables
├── scripts/                # Batch jobs (CLI rebuilds, exports)
├── requirements.txt
└── README.md
```

---

## 🔄 Data Sources

* **Equibase**: Plus Pro PPs, Results Charts (XML)
* **TPD** (Total Performance Data): GPS, Sectionals, Routes (JSON/KML)

> ⚠️ Note: Equibase data requires licensing. This repo contains loader stubs and schemas only.

---

## 🧠 Modeling Highlights

* **Predictive targets**:

  * Win/place likelihood
  * Top 2 ranking (Exacta use)
  * Speed/fatigue delta by surface
* **Feature types**:

  * Class drops/rises
  * Trainer/jockey stats by condition
  * Track-adjusted fractional performance
  * Pace shape + sectional timing trends
* **Models**:

  * LSTM for stride/fatigue sequence modeling
  * CatBoost + Logistic regression ensemble
  * Custom scoring metric for top 2 accuracy

---

## 📦 How to Run

```bash
conda activate foxriver
pip install -r requirements.txt
python scripts/load_all.py            # Load TPD + EQB data to DB
python scripts/train_model.py         # Build and save ensemble
python scripts/run_predict.py --race  # Predict upcoming race set
```

---

## 🔐 Licensing

Data sources used in this project (Equibase, TPD) may be subject to licensing and cannot be redistributed. This repository provides structure and tooling but does not contain proprietary data.

---

## 👤 Author

**Randall Shane** — [LinkedIn](https://www.linkedin.com/in/randall-shane/)

* Former systems architect, now applying AI to sports betting and real-world prediction systems
* Building private GenAI + ML pipelines optimized for niche, high-signal domains

---

## 📊 Roadmap

* [x] Full ingestion + DB tracking for TPD and EQB
* [x] LSTM modeling of stride/fatigue
* [x] Exacta and Pick 3 simulation
* [ ] GUI dashboard for model evaluation
* [ ] LangChain integration for racing insights Q\&A
* [ ] Discord bot / CLI interface for live picks
