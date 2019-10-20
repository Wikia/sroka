import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

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
    install_requires=requirements
)
