from setuptools import setup, find_packages

setup(
    name="athena_query_lib",
    version="1.0.0",
    description="A library for querying AWS Athena and saving results.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Anyamanee P.",
    author_email="anyamanee@techgateway.co.th",
    url="https://github.com/yourusername/athena-query-lib",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "pandas",
        "python-dotenv"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
