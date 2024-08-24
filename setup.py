import setuptools

packages = setuptools.find_packages(
    exclude=["tests"]
)

setuptools.setup(
    name="mongo-change-stream-mediator",
    description="Read your change stream under deployments, "
                "database or collection at-least-once and send to kafka",
    author="Evgenii M6",
    packages=packages,
    test_suite="tests",
)
