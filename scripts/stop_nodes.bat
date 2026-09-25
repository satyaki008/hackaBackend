@echo off
echo Stopping AegisStore processes...
taskkill /F /FI "WINDOWTITLE eq Node 1*" /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Node 2*" /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Node 3*" /T >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Node 4*" /T >nul 2>&1
echo Done.
