import os
from setuptools import setup, find_packages

with open(os.path.join('..', 'README.md'), "r") as file:
    read_me = file.read()

setup(
    name='mmproteo',
    version='0.2',
    description='Mirko meets Proteomics: A PRIDE downloader on steroids for deep learning-based DeNovo Sequencing',
    long_description=read_me,
    long_description_content_type="text/markdown",
    url='https://gitlab.com/dacs-hpi/pride-downloader',
    author='Mirko Krause',
    author_email='krause@codebase.one',
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',

)
