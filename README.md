# ResiStore Backend (Render Ready)

This is the standalone backend service for **ResiStore: Enterprise Self-Healing Distributed Object Storage System**.

## 🚀 One-Click Deploy to Render

### Option A: Deploy via GitHub (Recommended)
1. Push this repository to GitHub.
2. Go to your **[Render Dashboard](https://dashboard.render.com/)**.
3. Click **New +** $\rightarrow$ **Web Service**.
4. Connect your GitHub repository.
5. Configure the following settings:
   - **Name**: `resistore-backend`
   - **Region**: Closest to you (e.g., Singapore, Frankfurt, Oregon)
   - **Branch**: `main`
   - **Root Directory**: `backend` *(Leave empty if this folder is the root of its own repo)*
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
6. Click **Create Web Service**.
7. Once deployed, copy your Render URL (e.g. `https://resistore-backend.onrender.com`). You will paste this URL into your Vercel frontend environment variable.

### Option B: Deploy via Render Blueprint (`render.yaml`)
Render will automatically detect `render.yaml` inside this directory and configure the Python web service with zero manual typing required.

---

## 💻 Local Development

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Backend Gateway
```bash
python main.py
```
Or start the full cluster simulation (Gateway + 4 Nodes):
```bash
python scripts/run_system.py
```

### 3. Run Automated Tests (19/19 Passing)
```bash
python -m pytest
```

---

## 🌐 Endpoints Overview
- **Interactive Swagger Docs**: `http://localhost:8000/docs`
- **S3 API Gateway**: `/s3`
- **System Stats**: `/api/stats`
- **Nodes & Fault Injection**: `/api/nodes`
- **Objects & Replicas**: `/api/objects`
- **Reed-Solomon Erasure Coding**: `/api/resistore/erasure/matrix`
- **Chaos Monkey Arena**: `/api/resilience/chaos/trigger`
- **Performance Benchmark**: `/api/resilience/benchmark/run`
