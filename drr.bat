@echo off
:loop
xeno.exe
echo Re-run the program? (y/n)
set /p re_run=
if /i "%re_run%"=="y" goto loop
