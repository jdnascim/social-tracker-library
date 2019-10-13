# based on https://github.com/pypa/sampleproject/blob/master/setup.py

from setuptools import setup
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='social-tracker-library',  # Required

    version='0.2.19',  # Required

    description='Python library to communicate with the Social Tracker \
    application',  # Optional

    long_description=long_description,  # Optional

    long_description_content_type='text/markdown',  # Optional (see note above)

    url='https://github.com/jdnascim/social-tracker-library',  # Optional

    author='José Nascimento',  # Optional

    author_email='jose.dori.nascimento@gmail.com',  # Optional

    classifiers=[  # Optional
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Users of the Social Tracker Application',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.7',
    ],

    # packages=find_packages(exclude=['contrib', 'docs', 'tests']),  # Required
    packages=['social_tracker_library'],

    include_package_data=True,

    python_requires='>=3.5, <4',

    install_requires=required

)
