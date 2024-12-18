from setuptools import setup, find_packages

setup(
    name="py_redis_client",
    version="1.0.0.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
       "redis", "django>=4.2"],
    description="A helper library, built over redis-py, to use as cache, lock etc.",
    license='MIT',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author="Divyank Mishra",
    author_email="divyank.mishra@apnamart.in",
    url="https://github.com/Apna-Mart/py-redis-client",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Framework :: Django",
        "License :: OSI Approved :: MIT License"])
