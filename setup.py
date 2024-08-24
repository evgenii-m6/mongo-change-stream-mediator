import setuptools

packages = setuptools.find_packages(
    exclude=["tests"]
)

setuptools.setup(
    name="pymongo-change-stream-reader",
    description="Read your change stream under deployments, "
                "database or collection at-least-once and send to kafka",
    author="Evgenii M6",
    packages=packages,
    test_suite="tests",
)
