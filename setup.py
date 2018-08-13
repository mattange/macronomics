# -*- coding: utf-8 -*-
import setuptools

from macronomics.info import VERSION, PACKAGENAME, AUTHOR, AUTHOR_EMAIL

with open("README.rst", "r") as fh:
    long_description = fh.read()

#cmdclass = {'build_sphinx': BuildDoc}

setuptools.setup(
    name=PACKAGENAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    
    description="",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    
    url="https://github.com/mattange/" + PACKAGENAME,
    project_urls = {
            'Source': "https://github.com/mattange/" + PACKAGENAME,
            'Bug Reports': "https://github.com/mattange/" + PACKAGENAME + "/issues",
        },
    
    packages=setuptools.find_packages(exclude=['docs','examples','tests']),
    
    python_requires = '>=3.5',
    
    classifiers=(
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Natural Language :: English",
        "Topic :: Software Development :: Libraries"
    ),
    keywords='macronomics macroeconomics economics timeseries',
    
)

