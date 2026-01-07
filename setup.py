from setuptools import setup, find_packages

setup(
    name="cdc-system",
    version="1.0.0",
    description="Production-ready Change Data Capture system for Python",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary>=2.9.9",
        "mysql-connector-python>=8.2.0",
        "python-dotenv>=1.0.0",
    ],
    python_requires=">=3.8",
    entry_points={
        'console_scripts': [
            'cdc-demo=example_usage:run_basic_demo',
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)