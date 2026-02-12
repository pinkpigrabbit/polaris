# Backend (FastAPI + Temporal)

## 1) Python venv (독립 환경)

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements.txt -r requirements-dev.txt
```

macOS/Linux:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt -r requirements-dev.txt
```

## 2) Infra (Podman)

Temporal은 이미 별도 compose로 기동되어 있다고 가정합니다.

Polaris용 Postgres/Redis:

```bash
podman compose -f infra/podman-compose.polaris.yml up -d
```

## 3) DB 스키마 적용

컨테이너 내부에서 `db/schema.sql` 실행:

```bash
podman exec -i polaris-postgresql psql -U polaris -d polaris < db/schema.sql
```

## 4) 실행

환경변수는 `backend/.env.example` 참고.

API:

```bash
uvicorn app.main:app --app-dir backend --reload
```

Temporal worker:

```bash
python -m app.temporal.worker
```

## 5) 테스트

테스트는 (1) Postgres/Redis, (2) Temporal 서버, (3) 스키마 적용이 선행되어야 합니다.

```bash
pytest
```
