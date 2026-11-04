@echo off
setlocal
cd /d "%~dp0ide" || exit /b 1
pushd web
call npm ci || exit /b 1
call npm run build || exit /b 1
popd
echo Open http://127.0.0.1:8765
uv run --no-project --python 3.12 --with-requirements requirements.txt python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765
