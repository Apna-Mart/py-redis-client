from setuptools import setup, find_packages

setup(
    name="apna-redis",
    version="1.0.0",
    author="ApnaMart",
    author_email="tech@apnamart.in",
    description="A Redis-based cache library",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Apna-Mart/apna-redis",
    packages=find_packages(),
    install_requires=[
        "redis>=4.0.0",
    ],
    python_requires=">=3.6",
)
