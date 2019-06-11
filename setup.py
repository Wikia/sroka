import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sroka",
    version="0.0.2",
    author="Ad Engineering FANDOM",
    author_email="murbanek@fandom.com",
    description="Package for access GA, GAM, MOAT, Qubole, Athena, S3, Rubicon APIs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Wikia/sroka",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ),
    install_requires=[
        'google_auth_oauthlib>=0.2.0',
        'googleads>=14.1.0',
        'google_api_python_client>=1.6.7',
        'google-auth-httplib2>=0.0.3',
        'pandas>=0.23.4',
        'qds_sdk>=1.10.1',
        'boto3>=1.9.19',
        'retrying>=1.3.3',
        'pyarrow>=0.11.1',
        'botocore>=1.12.19',
        'numpy>=1.16.2',
        'urllib3>=1.24.2,<1.25',
        'requests>=2.20'
    ]
)
