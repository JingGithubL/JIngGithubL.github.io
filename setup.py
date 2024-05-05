from cx_Freeze import setup, Executable

setup(
    name="YourScript",
    version="1.0",
    description="Description of your script",
    executables=[Executable("OpenHtml.py")]
)