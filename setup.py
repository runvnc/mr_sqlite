from setuptools import setup, find_namespace_packages

setup(
    name="mr_sqlite",
    version="0.1.0",
    description="MindRoot SQLite Database Integration",
    author="MindRoot",
    author_email="info@mindroot.ai",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        "mr_sqlite": [
            "templates/*.jinja2",
            "static/js/*.js",
            "inject/*.jinja2",
            "override/*.jinja2"
        ],
    },
    install_requires=[
        # sqlite3 is part of the Python standard library
    ],
    python_requires=">=3.8",
)
