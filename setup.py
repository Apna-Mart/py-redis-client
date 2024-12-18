from setuptools import setup, find_packages

setup(
    name="py-redis-client",
    version="0.0.0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
       "redis", "Django"],
    description="A helper library, built over redis-py, to use as cache, lock etc.",
    author="Divyank Mishra",
    author_email="divyank.mishra@apnamart.in",
    url="https://github.com/Apna-Mart/py-redis-client",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Framework :: Django",
        "License :: OSI Approved :: MIT License",
    ])
