@echo off
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (py -3 -m venv .venv || exit /b 1)
.venv\Scripts\python -m pip install -r requirements.txt || exit /b 1
pushd web
call npm ci || exit /b 1
call npm run build || exit /b 1
popd
echo Open http://127.0.0.1:8765
.venv\Scripts\python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765
