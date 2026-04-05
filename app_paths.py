import os
import sys


if getattr(sys, "frozen", False):
    APP_DIR = os.path.dirname(sys.executable)
else:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))


CONFIG_FILENAME = "config.ini"
BOM_FILENAME = "BOM.csv"
DESCRIPTION_FILENAME = "Description.csv"
LOGO_FILENAME = "logo.png"

