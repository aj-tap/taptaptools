from setuptools import setup, find_packages

setup(
    name="taptaptools",
    version="0.1",
    author="aj-tap",
    author_email="devs@taptap.addymail.com",
    description="A jupyter notebooks for data analysis, threat detection, and automation.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/aj-tap/taptaptools",  # Replace with your actual repo URL
    packages=find_packages(where="src"),  # Ensuring it picks up modules inside 'src'
    package_dir={"": "src"},
    install_requires=[
        "certifi==2025.1.31",
        "charset-normalizer==3.4.1",
        "evtx",
        "idna==3.10",
        "Jinja2==3.1.5",
        "MarkupSafe==3.0.2",
        "numpy",
        "packaging==24.2",
        "pandas",
        "pyparsing==3.2.1",
        "pySigma==0.11.19",
        "pysigma-backend-carbonblack==0.1.8",
        "pysigma-backend-cortexxdr==0.1.4",
        "pySigma-backend-kusto==0.4.3",
        "pytz==2025.1",
        "PyYAML==6.0.2",
        "requests",
        "six==1.17.0",
        "tzdata==2025.1",
        "urllib3==2.3.0",
        "scikit-learn",
        "rrcf",
        "tensorflow"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
