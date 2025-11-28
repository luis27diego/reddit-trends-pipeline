@echo off
setlocal enableDelayedExpansion
set "CLOUDFLARED_EXE=cloudflared.exe"
set "CLOUDFLARED_LOG=%TEMP%\cloudflared_temp_log.txt"

echo.
echo ==========================================================
echo   PROYECTO MinIO + Cloudflare Tunnel
echo ==========================================================
echo.

:: 0. Limpieza preventiva (cerrar instancias viejas)
taskkill /f /im "%CLOUDFLARED_EXE%" >nul 2>&1

:: 1. Levantando MinIO
echo [1/3] Levantando MinIO (Docker Compose)...
docker compose up -d
timeout /t 5 >nul

echo.
:: 2. Generando tunel publico Cloudflare
echo [2/3] Generando tunel publico Cloudflare...

:: Borra el log anterior si existe
if exist "%CLOUDFLARED_LOG%" del "%CLOUDFLARED_LOG%"

:: Ejecuta cloudflared en SEGUNDO PLANO
start "Cloudflared Tunnel Log" /B cmd /c "%CLOUDFLARED_EXE% tunnel --url http://localhost:9000 --loglevel info > "%CLOUDFLARED_LOG%" 2>&1"

:: Espera a que el túnel se conecte
echo Esperando 15 segundos a que la URL aparezca...
timeout /t 15 >nul

:: ---------------------------------------------------------
:: LOGICA DE EXTRACCION DE URL MEJORADA (Anti-Error)
:: ---------------------------------------------------------
set "CLEAN_URL="

:: 1. Busca la línea que tiene "https://"
:: 2. Descompone la línea en palabras (tokens)
:: 3. Se queda SOLO con la palabra que empieza por "https://"
for /f "tokens=*" %%L in ('findstr /C:"https://" "%CLOUDFLARED_LOG%"') do (
    for %%W in (%%L) do (
        echo %%W | findstr "^https://" >nul
        if !errorlevel! equ 0 set "CLEAN_URL=%%W"
    )
)

echo.
:: 3. Mostrar la URL y copiarla
echo [3/3] Informacion del tunel:

if defined CLEAN_URL (
    echo.
    echo   -------------------------------------------------
    echo   TUNEL ACTIVO: !CLEAN_URL!
    echo   CONSOLE WEB:  !CLEAN_URL!:9001
    echo   -------------------------------------------------
    echo.
    
    :: Copia al portapapeles
    echo !CLEAN_URL!| clip
    echo   (URL copiada al portapapeles)
) else (
    echo.
    echo   ERROR: No se pudo capturar la URL.
    echo   Es posible que el tunel no haya iniciado correctamente.
    echo   Revisa el archivo: %CLOUDFLARED_LOG%
)

echo.
echo ==========================================================
echo Presiona cualquier tecla para apagar MinIO y el tunel...
pause >nul

echo.
echo Apagando MinIO y tunel...
taskkill /f /im "%CLOUDFLARED_EXE%" >nul 2>&1
docker compose down

if exist "%CLOUDFLARED_LOG%" del "%CLOUDFLARED_LOG%" >nul 2>&1
echo Todo apagado.
endlocal