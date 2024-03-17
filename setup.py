import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

PROJECT_NAME = "zomato_case_study"
USERNAME = "arjunaju123"

setuptools.setup(
    name="src", # The name of the package, which is the name that pip will use for your package.
    version="0.0.1", # The version of your package. This is the version pip will report, and is used for example when you publish your package on PyPI1.
    author=USERNAME,
    author_email="54721arjun@gmail.com",
    description="Implementation of (ETL) processes and predictive analysis on the Zomato dataset.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github.com/{USERNAME}/{PROJECT_NAME}",
    packages=["src"], #same as name
    python_requires=">=3.7",
    install_requires=["beautifulsoup4", 
                      "pandas", 
                      "requests", 
                      "tqdm", 
                      "joblib", 
                      "sqlalchemy", 
                      "pymysql", 
                      "scipy", 
                      "scikit-learn", 
                      "plotly", 
                      "matplotlib", 
                      "PyYAML"]

)