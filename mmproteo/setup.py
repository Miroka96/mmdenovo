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
    license='GPLv3+',
    packages=find_packages(),
    entry_points={
        "console_scripts": ['mmproteo = mmproteo.mmproteo:main']
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Environment :: Console",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "requests~=2.22.0",
        "pandas~=1.1.3",
        "pyteomics~=4.4.1",
        "wget~=3.2",
        "pyarrow~=2.0.0",
        "lxml~=4.5.0",
        "pytest~=6.2.1",
    ]
)
