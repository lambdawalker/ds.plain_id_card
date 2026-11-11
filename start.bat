@echo off
setlocal
set "PROJECT_DIR=%~dp0"

if exist "%PROJECT_DIR%venv\Scripts\python.exe" (
    "%PROJECT_DIR%venv\Scripts\python.exe" "%PROJECT_DIR%start.py" %*
) else if exist "%PROJECT_DIR%.venv\Scripts\python.exe" (
    "%PROJECT_DIR%.venv\Scripts\python.exe" "%PROJECT_DIR%start.py" %*
) else (
    py -3 "%PROJECT_DIR%start.py" %*
)

exit /b %errorlevel%
