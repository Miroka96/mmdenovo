import os
from setuptools import setup, find_packages

with open(os.path.join('..', 'README.md')) as f:
    read_me = f.read()

setup(
    name='mmpro',
    version='0.1',
    description='Mirko meets Proteomics: Deep learning based DeNovo Sequencing and Tools',
    long_description=read_me,
    url='https://github.com/Miroka96/de-novo-sequencing',
    author='Mirko Krause',
    author_email='krause@codebase.one',
    packages=find_packages(),
)