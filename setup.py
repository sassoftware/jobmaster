from setuptools import setup, find_packages, extension
setup(name="jobmaster", version="5.2",
        packages=find_packages(
            exclude=('jobmaster_test', 'jobmaster_test.*')),
        ext_modules=[
            extension.Extension('jobmaster.linuxns', ['src/linuxns.c']),
            extension.Extension('jobmaster.osutil', ['src/osutil.c']),
            ],
        )
